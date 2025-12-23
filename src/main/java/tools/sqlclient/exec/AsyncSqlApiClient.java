package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.network.TrustAllHttpClient;
import tools.sqlclient.util.OperationLog;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 统一封装异步 SQL HTTP 调用，适配 HTTP 410、RESULT_NOT_READY、overloaded 等分支。
 */
public class AsyncSqlApiClient {
    private static final int MAX_LOG_TEXT_LENGTH = 200_000;
    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Gson gson = new Gson();

    public SubmitResponse submit(String sql, String dbUser, String label, Integer maxResultRows, Integer pageSize) {
        JsonObject body = new JsonObject();
        body.addProperty("encryptedSql", AesCbcBase64.encrypt(sql));
        if (dbUser != null && !dbUser.isBlank()) {
            body.addProperty("dbUser", dbUser);
        }
        if (label != null && !label.isBlank()) {
            body.addProperty("label", label);
        }
        if (maxResultRows != null && maxResultRows > 0) {
            body.addProperty("maxResultRows", maxResultRows);
        }
        if (pageSize != null && pageSize > 0) {
            body.addProperty("pageSize", pageSize);
        }
        JsonObject resp = postJson("/jobs/submit", body, false);
        SubmitResponse sr = parseSubmit(resp);
        if (Boolean.FALSE.equals(sr.getSuccess()) || Boolean.TRUE.equals(sr.getOverloaded())) {
            throw new OverloadedException(sr.getMessage() != null ? sr.getMessage() : "系统过载，提交被拒绝");
        }
        return sr;
    }

    public ResultResponse fetchResult(String jobId, int page, int pageSize, boolean removeAfterFetch) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        body.addProperty("page", page);
        body.addProperty("pageSize", pageSize);
        body.addProperty("removeAfterFetch", removeAfterFetch);
        boolean allow410 = true;
        JsonObject resp = postJson("/jobs/result", body, allow410);
        ResultResponse rr = parseResult(resp);
        if (Boolean.TRUE.equals(rr.getOverloaded())) {
            throw new OverloadedException(rr.getMessage() != null ? rr.getMessage() : "系统过载");
        }
        if (rr.getCode() != null && "RESULT_EXPIRED".equalsIgnoreCase(rr.getCode())) {
            throw new ResultExpiredException(jobId, rr.getMessage() != null ? rr.getMessage() : "结果已过期");
        }
        if (rr.getCode() != null && "RESULT_NOT_READY".equalsIgnoreCase(rr.getCode())) {
            throw new ResultNotReadyException(jobId, rr.getMessage() != null ? rr.getMessage() : "结果暂不可用");
        }
        return rr;
    }

    public StatusResponse status(String jobId) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        JsonObject resp = postJson("/jobs/status", body, false);
        return parseStatus(resp);
    }

    private JsonObject postJson(String path, JsonObject payload, boolean allow410) {
        try {
            String jsonBody = gson.toJson(payload);
            HttpRequest request = HttpRequest.newBuilder(AsyncSqlConfig.buildUri(path))
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .header("X-Request-Token", AsyncSqlConfig.REQUEST_TOKEN)
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();
            logHttpRequest(request, jsonBody);
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int sc = resp.statusCode();
            logHttpResponse(request.uri().toString(), sc, resp.headers(), resp.body());
            if (allow410 && sc == 410) {
                JsonObject obj = gson.fromJson(resp.body(), JsonObject.class);
                if (obj == null) {
                    obj = new JsonObject();
                }
                if (!obj.has("code") || obj.get("code").isJsonNull()) {
                    obj.addProperty("code", "RESULT_EXPIRED");
                }
                obj.addProperty("status", "EXPIRED");
                return obj;
            }
            if (sc / 100 != 2) {
                throw new RuntimeException("HTTP " + sc + " | " + resp.body());
            }
            return gson.fromJson(resp.body(), JsonObject.class);
        } catch (ResultExpiredException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("请求失败: " + e.getMessage(), e);
        }
    }

    private SubmitResponse parseSubmit(JsonObject obj) {
        SubmitResponse sr = new SubmitResponse();
        if (obj == null) return sr;
        sr.setJobId(readString(obj, "jobId"));
        sr.setSuccess(readBoolean(obj, "success"));
        sr.setOverloaded(readBoolean(obj, "overloaded"));
        sr.setMessage(readString(obj, "message"));
        sr.setQueuedAt(readLong(obj, "queuedAt"));
        sr.setQueueDelayMillis(readLong(obj, "queueDelayMillis"));
        sr.setThreadPool(parseThreadPool(obj));
        sr.setCode(readString(obj, "code"));
        return sr;
    }

    private StatusResponse parseStatus(JsonObject obj) {
        StatusResponse sr = new StatusResponse();
        if (obj == null) return sr;
        sr.setJobId(readString(obj, "jobId"));
        sr.setSuccess(readBoolean(obj, "success"));
        sr.setStatus(readString(obj, "status"));
        sr.setProgressPercent(readInt(obj, "progressPercent"));
        sr.setElapsedMillis(readLong(obj, "elapsedMillis"));
        sr.setOverloaded(readBoolean(obj, "overloaded"));
        sr.setQueuedAt(readLong(obj, "queuedAt"));
        sr.setQueueDelayMillis(readLong(obj, "queueDelayMillis"));
        sr.setThreadPool(parseThreadPool(obj));
        sr.setMessage(readString(obj, "message"));
        return sr;
    }

    private ResultResponse parseResult(JsonObject obj) {
        ResultResponse rr = new ResultResponse();
        if (obj == null) return rr;
        rr.setJobId(readString(obj, "jobId"));
        rr.setSuccess(readBoolean(obj, "success"));
        rr.setStatus(readString(obj, "status"));
        rr.setCode(readString(obj, "code"));
        rr.setResultAvailable(readBoolean(obj, "resultAvailable"));
        rr.setArchived(readBoolean(obj, "archived"));
        rr.setArchiveError(readString(obj, "archiveError"));
        rr.setExpiresAt(readLong(obj, "expiresAt"));
        rr.setLastAccessAt(readLong(obj, "lastAccessAt"));
        rr.setOverloaded(readBoolean(obj, "overloaded"));
        rr.setQueueDelayMillis(readLong(obj, "queueDelayMillis"));
        rr.setReturnedRowCount(readInt(obj, "returnedRowCount"));
        rr.setActualRowCount(readInt(obj, "actualRowCount"));
        rr.setMaxVisibleRows(readInt(obj, "maxVisibleRows"));
        rr.setMaxTotalRows(readInt(obj, "maxTotalRows"));
        rr.setHasResultSet(readBoolean(obj, "hasResultSet"));
        rr.setPage(readInt(obj, "page"));
        rr.setPageSize(readInt(obj, "pageSize"));
        rr.setHasNext(readBoolean(obj, "hasNext"));
        rr.setTruncated(readBoolean(obj, "truncated"));
        rr.setMessage(readString(obj, "message"));
        rr.setThreadPool(parseThreadPool(obj));
        rr.setColumns(extractColumns(obj));
        JsonArray rows = extractRows(obj);
        rr.setRawRows(rows);
        rr.setRowMaps(extractRowMaps(rows, rr.getColumns()));
        return rr;
    }

    private List<String> extractColumns(JsonObject obj) {
        List<String> columns = new ArrayList<>();
        if (obj == null) return columns;
        for (String key : List.of("columns", "resultColumns", "columnNames")) {
            if (obj.has(key) && obj.get(key).isJsonArray()) {
                for (JsonElement el : obj.getAsJsonArray(key)) {
                    if (el != null && el.isJsonPrimitive()) {
                        columns.add(el.getAsString());
                    }
                }
                if (!columns.isEmpty()) {
                    return columns;
                }
            }
        }
        JsonArray rows = extractRows(obj);
        if (rows != null && rows.size() > 0 && rows.get(0).isJsonObject()) {
            for (Map.Entry<String, JsonElement> entry : rows.get(0).getAsJsonObject().entrySet()) {
                columns.add(entry.getKey());
            }
        } else if (rows != null && rows.size() > 0 && rows.get(0).isJsonArray()) {
            int size = rows.get(0).getAsJsonArray().size();
            for (int i = 0; i < size; i++) {
                columns.add("col_" + (i + 1));
            }
        }
        return columns;
    }

    private JsonArray extractRows(JsonObject obj) {
        if (obj == null) {
            return new JsonArray();
        }
        for (String key : List.of("resultRows", "rows", "data")) {
            if (obj.has(key) && obj.get(key).isJsonArray()) {
                return obj.getAsJsonArray(key);
            }
        }
        return new JsonArray();
    }

    private List<java.util.Map<String, String>> extractRowMaps(JsonArray rowsJson, List<String> columns) {
        List<java.util.Map<String, String>> rows = new ArrayList<>();
        if (rowsJson == null) {
            return rows;
        }
        int colCount = columns == null ? 0 : columns.size();
        for (JsonElement el : rowsJson) {
            if (el == null || el.isJsonNull()) {
                continue;
            }
            if (el.isJsonObject()) {
                JsonObject obj = el.getAsJsonObject();
                java.util.Map<String, String> row = new java.util.LinkedHashMap<>();
                for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                    row.put(entry.getKey(), stringify(entry.getValue()));
                }
                rows.add(row);
            } else if (el.isJsonArray()) {
                JsonArray arr = el.getAsJsonArray();
                java.util.Map<String, String> row = new java.util.LinkedHashMap<>();
                for (int i = 0; i < arr.size(); i++) {
                    String key = i < colCount ? columns.get(i) : ("col_" + (i + 1));
                    row.put(key, stringify(arr.get(i)));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private String stringify(JsonElement v) {
        if (v == null || v.isJsonNull()) {
            return null;
        }
        if (v.isJsonPrimitive()) {
            return v.getAsString();
        }
        return gson.toJson(v);
    }

    private String readString(JsonObject obj, String key) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return null;
        return obj.get(key).getAsString();
    }

    private Integer readInt(JsonObject obj, String key) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return null;
        try {
            return obj.get(key).getAsInt();
        } catch (Exception e) {
            return null;
        }
    }

    private Long readLong(JsonObject obj, String key) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return null;
        try {
            return obj.get(key).getAsLong();
        } catch (Exception e) {
            return null;
        }
    }

    private Boolean readBoolean(JsonObject obj, String key) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return null;
        try {
            return obj.get(key).getAsBoolean();
        } catch (Exception e) {
            return null;
        }
    }

    private ThreadPoolSnapshot parseThreadPool(JsonObject obj) {
        if (obj == null || !obj.has("threadPool") || obj.get("threadPool").isJsonNull()) {
            return null;
        }
        JsonObject tp = obj.getAsJsonObject("threadPool");
        Integer poolSize = readInt(tp, "poolSize");
        Integer activeCount = readInt(tp, "activeCount");
        Integer queueSize = readInt(tp, "queueSize");
        Long taskCount = readLong(tp, "taskCount");
        Long completedTaskCount = readLong(tp, "completedTaskCount");
        Integer jobStoreSize = readInt(tp, "jobStoreSize");
        return new ThreadPoolSnapshot(poolSize, activeCount, queueSize, taskCount, completedTaskCount, jobStoreSize);
    }

    private void logHttpRequest(HttpRequest request, String body) {
        if (!OperationLog.isReady()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("--- HTTP REQUEST BEGIN ---\n");
        sb.append(request.method()).append(' ').append(request.uri()).append('\n');
        sb.append("Headers:\n");
        request.headers().map().forEach((k, v) -> sb.append("  ").append(k).append(": ")
                .append(String.join(",", v)).append('\n'));
        sb.append("Body:\n");
        sb.append(body == null ? "<empty>" : body).append('\n');
        sb.append("--- HTTP REQUEST END ---");
        OperationLog.log(sb.toString());
    }

    private void logHttpResponse(String url, int status, HttpHeaders headers, String body) {
        if (!OperationLog.isReady()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("--- HTTP RESPONSE BEGIN ---\n");
        sb.append("URL: ").append(url).append('\n');
        sb.append("Status: ").append(status).append('\n');
        sb.append("Headers:\n");
        headers.map().forEach((k, v) -> sb.append("  ").append(k).append(": ")
                .append(String.join(",", v)).append('\n'));
        sb.append("Body:\n");

        String fullBody = body == null ? "" : body;
        if (fullBody.length() > MAX_LOG_TEXT_LENGTH) {
            String truncated = fullBody.substring(0, MAX_LOG_TEXT_LENGTH);
            sb.append(truncated);
            sb.append("\n<响应体超长，已截断>");
        } else {
            sb.append(fullBody);
        }
        sb.append('\n').append("--- HTTP RESPONSE END ---");
        OperationLog.log(sb.toString());
    }
}
