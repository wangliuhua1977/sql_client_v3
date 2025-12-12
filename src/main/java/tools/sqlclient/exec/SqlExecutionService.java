package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.network.TrustAllHttpClient;
import tools.sqlclient.util.AesEncryptor;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.ThreadPools;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * 基于异步任务接口的 SQL 执行服务：提交任务、轮询状态、获取结果。
 */
public class SqlExecutionService {
    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Gson gson = new Gson();

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError) {
        return execute(sql, onSuccess, onError, null);
    }

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError,
                                           Consumer<AsyncJobStatus> onStatus) {
        String trimmed = sql == null ? "" : sql.trim();
        if (trimmed.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        OperationLog.log("即将提交异步 SQL:\n" + abbreviate(trimmed));

        return CompletableFuture.supplyAsync(() -> submitJob(trimmed), ThreadPools.NETWORK_POOL)
                .thenCompose(submit -> {
                    notifyStatus(onStatus, submit);
                    return pollJobUntilDone(submit.getJobId(), onStatus);
                })
                .thenCompose(status -> {
                    if ("SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
                        return CompletableFuture.supplyAsync(() -> requestResult(status.getJobId(), trimmed), ThreadPools.NETWORK_POOL)
                                .thenAccept(res -> {
                                    notifyStatus(onStatus, status);
                                    if (onSuccess != null) {
                                        onSuccess.accept(res);
                                    }
                                });
                    }
                    RuntimeException failure = new RuntimeException(status.getMessage() != null
                            ? status.getMessage()
                            : ("任务" + status.getJobId() + " 状态 " + status.getStatus()));
                    if (onError != null) {
                        onError.accept(failure);
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .exceptionally(ex -> {
                    if (onError != null) {
                        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                        onError.accept(new RuntimeException(cause));
                    }
                    return null;
                });
    }

    public CompletableFuture<AsyncJobStatus> cancelJob(String jobId, String reason) {
        return CompletableFuture.supplyAsync(() -> doCancel(jobId, reason), ThreadPools.NETWORK_POOL);
    }

    public CompletableFuture<List<AsyncJobStatus>> listJobs() {
        return CompletableFuture.supplyAsync(this::doList, ThreadPools.NETWORK_POOL);
    }

    private AsyncJobStatus submitJob(String sql) {
        JsonObject body = new JsonObject();
        body.addProperty("encryptedSql", AesEncryptor.encryptSqlToBase64(sql));
        body.addProperty("maxResultRows", 100);

        OperationLog.log("提交 /jobs/submit ...");
        JsonObject resp = postJson("/jobs/submit", body);

        AsyncJobStatus status = parseStatus(resp);
        OperationLog.log("[" + status.getJobId() + "] 已提交，状态 " + status.getStatus());
        return status;
    }



    private CompletableFuture<AsyncJobStatus> pollJobUntilDone(String jobId, Consumer<AsyncJobStatus> onStatus) {
        return CompletableFuture.supplyAsync(() -> {
            AsyncJobStatus latest = null;
            boolean finished = false;
            while (!finished) {
                latest = requestStatus(jobId);
                notifyStatus(onStatus, latest);
                finished = isTerminal(latest.getStatus());
                if (!finished) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("轮询中断", e);
                    }
                }
            }
            return latest;
        }, ThreadPools.NETWORK_POOL);
    }

    private AsyncJobStatus requestStatus(String jobId) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        JsonObject resp = postJson("/jobs/status", body);
        AsyncJobStatus status = parseStatus(resp);
        Integer progress = status.getProgressPercent();
        String progressText = progress != null ? progress + "%" : "-";
        OperationLog.log("[" + jobId + "] 状态 " + status.getStatus() + " 进度 " + progressText);
        return status;
    }

    private SqlExecResult requestResult(String jobId, String sql) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        body.addProperty("removeAfterFetch", true);
        JsonObject resp = postJson("/jobs/result", body);
        boolean success = resp.has("success") && resp.get("success").getAsBoolean();
        String status = resp.has("status") && !resp.get("status").isJsonNull() ? resp.get("status").getAsString() : "";
        Integer progress = resp.has("progressPercent") && !resp.get("progressPercent").isJsonNull()
                ? resp.get("progressPercent").getAsInt() : null;
        Long duration = resp.has("durationMillis") && !resp.get("durationMillis").isJsonNull()
                ? resp.get("durationMillis").getAsLong() : null;
        Integer rowsAffected = resp.has("rowsAffected") && !resp.get("rowsAffected").isJsonNull()
                ? resp.get("rowsAffected").getAsInt() : null;
        Integer returnedRowCount = resp.has("returnedRowCount") && !resp.get("returnedRowCount").isJsonNull()
                ? resp.get("returnedRowCount").getAsInt() : null;
        Boolean hasResultSet = resp.has("hasResultSet") && !resp.get("hasResultSet").isJsonNull()
                ? resp.get("hasResultSet").getAsBoolean() : null;
        String message = extractMessage(resp);

        if (!success) {
            OperationLog.log("[" + jobId + "] 任务失败/未完成: " + message);
            throw new RuntimeException(message);
        }

        if (hasResultSet != null && !hasResultSet) {
            List<String> columns = List.of("消息");
            List<List<String>> rows = new ArrayList<>();
            String info = rowsAffected != null ? ("影响行数: " + rowsAffected) : (message != null ? message : "执行成功");
            rows.add(List.of(info));
            return new SqlExecResult(sql, columns, rows, rows.size(), true, message, jobId, status, progress,
                    null, duration, rowsAffected, returnedRowCount, hasResultSet);
        }

        JsonArray rowsJson = resp.has("resultRows") && resp.get("resultRows").isJsonArray()
                ? resp.getAsJsonArray("resultRows") : new JsonArray();
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        if (rowsJson.size() > 0) {
            JsonObject first = rowsJson.get(0).getAsJsonObject();
            for (Map.Entry<String, JsonElement> entry : first.entrySet()) {
                columns.add(entry.getKey());
            }
            for (JsonElement el : rowsJson) {
                if (el != null && el.isJsonObject()) {
                    JsonObject obj = el.getAsJsonObject();
                    List<String> row = new ArrayList<>();
                    for (String col : columns) {
                        JsonElement v = obj.get(col);
                        row.add(v == null || v.isJsonNull() ? "" : v.getAsString());
                    }
                    rows.add(row);
                }
            }
        }
        int rowCount = returnedRowCount != null ? returnedRowCount : rows.size();
        OperationLog.log("[" + jobId + "] 任务完成，行数 " + rowCount);
        return new SqlExecResult(sql, columns, rows, rowCount, true, message, jobId, status, progress,
                null, duration, rowsAffected, returnedRowCount, hasResultSet);
    }

    private AsyncJobStatus doCancel(String jobId, String reason) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        if (reason != null) {
            body.addProperty("reason", reason);
        }
        JsonObject resp = postJson("/jobs/cancel", body);
        AsyncJobStatus status = parseStatus(resp);
        OperationLog.log("[" + jobId + "] 取消请求已发送，状态 " + status.getStatus());
        return status;
    }

    private List<AsyncJobStatus> doList() {
        JsonObject body = new JsonObject();
        JsonObject resp = postJson("/jobs/list", body);
        List<AsyncJobStatus> list = new ArrayList<>();
        if (resp.has("jobs") && resp.get("jobs").isJsonArray()) {
            for (JsonElement el : resp.get("jobs").getAsJsonArray()) {
                if (el != null && el.isJsonObject()) {
                    list.add(parseStatus(el.getAsJsonObject()));
                }
            }
        }
        return list;
    }

    private boolean isTerminal(String status) {
        if (status == null) return true;
        String s = status.toUpperCase();
        return "SUCCEEDED".equals(s) || "FAILED".equals(s) || "CANCELLED".equals(s);
    }

    private AsyncJobStatus parseStatus(JsonObject obj) {
        String jobId = obj.has("jobId") && !obj.get("jobId").isJsonNull() ? obj.get("jobId").getAsString() : "";
        String status = obj.has("status") && !obj.get("status").isJsonNull() ? obj.get("status").getAsString() : "";
        Integer progress = obj.has("progressPercent") && !obj.get("progressPercent").isJsonNull()
                ? obj.get("progressPercent").getAsInt() : null;
        Long elapsed = obj.has("elapsedMillis") && !obj.get("elapsedMillis").isJsonNull()
                ? obj.get("elapsedMillis").getAsLong() : null;
        String label = obj.has("label") && !obj.get("label").isJsonNull() ? obj.get("label").getAsString() : null;
        String sqlSummary = obj.has("sqlSummary") && !obj.get("sqlSummary").isJsonNull() ? obj.get("sqlSummary").getAsString() : null;
        Integer rowsAffected = obj.has("rowsAffected") && !obj.get("rowsAffected").isJsonNull()
                ? obj.get("rowsAffected").getAsInt() : null;
        Integer returnedRowCount = obj.has("returnedRowCount") && !obj.get("returnedRowCount").isJsonNull()
                ? obj.get("returnedRowCount").getAsInt() : null;
        Boolean hasResultSet = obj.has("hasResultSet") && !obj.get("hasResultSet").isJsonNull()
                ? obj.get("hasResultSet").getAsBoolean() : null;
        String message = extractMessage(obj);
        return new AsyncJobStatus(jobId, status, progress, elapsed, label, sqlSummary, rowsAffected, returnedRowCount, hasResultSet, message);
    }

    private JsonObject postJson(String path, JsonObject payload) {
        try {
            HttpRequest request = HttpRequest.newBuilder(AsyncSqlConfig.buildUri(path))
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .header("X-Request-Token", AsyncSqlConfig.REQUEST_TOKEN)
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(payload)))
                    .build();

            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int sc = resp.statusCode();

            // 任何 2xx 都视为成功（包括 202 Accepted）
            if (sc / 100 != 2) {
                String body = resp.body();
                throw new RuntimeException("HTTP " + sc + " | " + body);
            }

            return gson.fromJson(resp.body(), JsonObject.class);
        } catch (Exception e) {
            throw new RuntimeException("请求失败: " + e.getMessage(), e);
        }
    }


    private void notifyStatus(Consumer<AsyncJobStatus> onStatus, AsyncJobStatus status) {
        if (onStatus != null && status != null) {
            onStatus.accept(status);
        }
    }

    private String extractMessage(JsonObject obj) {
        if (obj == null) return null;
        if (obj.has("message") && !obj.get("message").isJsonNull()) {
            return obj.get("message").getAsString();
        }
        if (obj.has("note") && !obj.get("note").isJsonNull()) {
            return obj.get("note").getAsString();
        }
        if (obj.has("errorMessage") && !obj.get("errorMessage").isJsonNull()) {
            return obj.get("errorMessage").getAsString();
        }
        return null;
    }

    private String abbreviate(String text) {
        if (text == null) {
            return "";
        }
        String t = text.strip();
        return t.length() > 600 ? t.substring(0, 600) + "..." : t;
    }
}
