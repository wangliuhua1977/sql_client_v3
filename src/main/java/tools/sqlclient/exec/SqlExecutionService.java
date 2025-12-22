package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.network.TrustAllHttpClient;
import tools.sqlclient.util.AesEncryptor;
import tools.sqlclient.util.Config;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.ThreadPools;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 基于异步任务接口的 SQL 执行服务：提交任务、轮询状态、获取结果。
 */
public class SqlExecutionService {
    private static final int DEFAULT_MAX_RESULT_ROWS = 0; // 分页模式下不再按总行数截断
    private static final int DEFAULT_PAGE_SIZE = 200;
    private static final int MAX_PAGE_SIZE = Math.max(1, Config.getMaxPageSize());
    private static final int MAX_ACCUMULATED_PAGES = 20;
    private static final int MAX_ACCUMULATED_ROWS = 100_000;
    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Gson gson = new Gson();

    private static final int MAX_LOG_TEXT_LENGTH = 200_000;

    /**
     * 同步执行一条 SQL：提交后台任务，轮询至结束并返回结果集。
     * 仍然复用异步接口的加密与 Header 逻辑。
     */
    public SqlExecResult executeSync(String sql) {
        return executeSync(sql, DEFAULT_MAX_RESULT_ROWS);
    }

    public SqlExecResult executeSyncWithDbUser(String sql, String dbUser) {
        return executeSync(sql, DEFAULT_MAX_RESULT_ROWS, false, dbUser);
    }

    public SqlExecResult executeSyncWithoutLimit(String sql) {
        return executeSync(sql, null);
    }

    public SqlExecResult executeSyncAllPages(String sql) {
        return executeSync(sql, null, true);
    }

    public SqlExecResult executeSyncAllPagesWithDbUser(String sql, String dbUser) {
        return executeSync(sql, null, true, dbUser);
    }

    public SqlExecResult executeSyncAllPagesWithDbUser(String sql, String dbUser, Integer pageSizeOverride) {
        return executeSync(sql, null, true, dbUser, pageSizeOverride);
    }

    public SqlExecResult executeSync(String sql, Integer maxResultRows) {
        return executeSync(sql, maxResultRows, false);
    }

    private SqlExecResult executeSync(String sql, Integer maxResultRows, boolean fetchAllPages) {
        return executeSync(sql, maxResultRows, fetchAllPages, null);
    }

    private SqlExecResult executeSync(String sql, Integer maxResultRows, boolean fetchAllPages, String dbUser) {
        return executeSync(sql, maxResultRows, fetchAllPages, dbUser, null);
    }

    private SqlExecResult executeSync(String sql, Integer maxResultRows, boolean fetchAllPages, String dbUser, Integer pageSizeOverride) {
        int resolvedMaxRows = determineMaxRows(maxResultRows, pageSizeOverride);
        AsyncJobStatus submitted = submitJob(sql, resolvedMaxRows, dbUser, null);
        try {
            AsyncJobStatus finalStatus = pollJobUntilDone(submitted.getJobId(), null).join();
            if (!"SUCCEEDED".equalsIgnoreCase(finalStatus.getStatus())) {
                throw new RuntimeException(finalStatus.getMessage() != null
                        ? finalStatus.getMessage()
                        : ("任务" + finalStatus.getJobId() + " 状态 " + finalStatus.getStatus()));
            }
            return requestResultPaged(submitted.getJobId(), sql, fetchAllPages, pageSizeOverride, finalStatus);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("执行 SQL 失败: " + cause.getMessage(), cause);
        }
    }

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError) {
        return execute(sql, null, null, onSuccess, onError, null);
    }

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError,
                                           Consumer<AsyncJobStatus> onStatus) {
        return execute(sql, null, null, onSuccess, onError, onStatus);
    }

    public CompletableFuture<Void> execute(String sql,
                                           String dbUser,
                                           Integer pageSize,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError,
                                           Consumer<AsyncJobStatus> onStatus) {
        String trimmed = sql == null ? "" : sql.trim();
        if (trimmed.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        OperationLog.log("即将提交异步 SQL:\n" + abbreviate(trimmed));

        return CompletableFuture.supplyAsync(() -> submitJob(trimmed, determineMaxRows(DEFAULT_MAX_RESULT_ROWS, pageSize), dbUser, null), ThreadPools.NETWORK_POOL)
                .thenCompose(submit -> {
                    notifyStatus(onStatus, submit);
                    return pollJobUntilDone(submit.getJobId(), onStatus);
                })
                .thenCompose(status -> {
                    if ("SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
                        return CompletableFuture.supplyAsync(() -> requestResultPaged(status.getJobId(), trimmed, false, pageSize, status), ThreadPools.NETWORK_POOL)
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
        return submitJob(sql, determineMaxRows(DEFAULT_MAX_RESULT_ROWS, null), null, null);
    }

    private AsyncJobStatus submitJob(String sql, Integer maxResultRows, String dbUser, String label) {
        JsonObject body = new JsonObject();
        body.addProperty("encryptedSql", AesEncryptor.encryptSqlToBase64(sql));
        if (maxResultRows != null && maxResultRows > 0) {
            body.addProperty("maxResultRows", maxResultRows);
        }
        if (dbUser != null && !dbUser.isBlank()) {
            body.addProperty("dbUser", dbUser);
        }
        if (label != null && !label.isBlank()) {
            body.addProperty("label", label);
        }

        OperationLog.log("提交 /jobs/submit ... dbUser=" + (dbUser == null ? "<default>" : dbUser)
                + (maxResultRows != null && maxResultRows > 0 ? (" maxResultRows=" + maxResultRows) : "")
                + (label != null && !label.isBlank() ? (" label=" + label) : ""));
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

    public SqlExecResult requestResultPaged(String jobId, String sql, boolean fetchAllPages, Integer pageSizeOverride) {
        return requestResultPaged(jobId, sql, fetchAllPages, pageSizeOverride, null);
    }

    public SqlExecResult requestResultPaged(String jobId, String sql, boolean fetchAllPages, Integer pageSizeOverride, AsyncJobStatus finalStatus) {
        int pageSize = resolvePageSize(pageSizeOverride);
        ResultFetchContext context = new ResultFetchContext(AsyncResultConfig.getPageBase(),
                AsyncResultConfig.getRetryMaxAttempts(),
                AsyncResultConfig.getRetryBaseDelayMs(),
                AsyncResultConfig.getRetryMaxDelayMs(),
                AsyncResultConfig.getRemoveAfterFetchStrategy());
        int pageIndex = 0; // zero-based index for client side
        SqlExecResult firstPage = requestResultPageWithRetry(jobId, sql, pageIndex, pageSize, context, finalStatus);
        if (!fetchAllPages || firstPage == null) {
            context.cleanupResultIfNeeded(jobId, pageSize);
            return firstPage;
        }

        List<List<String>> allRows = new ArrayList<>();
        List<java.util.Map<String, String>> allRowMaps = new ArrayList<>();
        if (firstPage.getRows() != null) {
            allRows.addAll(firstPage.getRows());
        }
        if (firstPage.getRowMaps() != null) {
            allRowMaps.addAll(firstPage.getRowMaps());
        }

        boolean hasNext = Boolean.TRUE.equals(firstPage.getHasNext());
        boolean truncated = Boolean.TRUE.equals(firstPage.getTruncated());
        String note = firstPage.getNote();
        boolean stoppedByLimit = false;

        while (hasNext && pageIndex + 1 < MAX_ACCUMULATED_PAGES && allRows.size() < MAX_ACCUMULATED_ROWS) {
            pageIndex++;
            SqlExecResult pageResult = requestResultPageWithRetry(jobId, sql, pageIndex, pageSize, context, finalStatus);
            if (pageResult.getRows() != null) {
                allRows.addAll(pageResult.getRows());
            }
            if (pageResult.getRowMaps() != null) {
                allRowMaps.addAll(pageResult.getRowMaps());
            }
            if (pageResult.getTruncated() != null && pageResult.getTruncated()) {
                truncated = true;
            }
            if (pageResult.getNote() != null && !pageResult.getNote().isBlank()) {
                note = pageResult.getNote();
            }
            hasNext = Boolean.TRUE.equals(pageResult.getHasNext());
            if (allRows.size() >= MAX_ACCUMULATED_ROWS) {
                stoppedByLimit = true;
                break;
            }
        }

        if (stoppedByLimit && hasNext) {
            OperationLog.log("[" + jobId + "] 达到分页累积上限，已停止继续拉取剩余数据");
        }

        context.cleanupResultIfNeeded(jobId, pageSize);
        return new SqlExecResult(sql, firstPage.getColumns(), firstPage.getColumnDefs(), allRows, allRowMaps, allRows.size(), true, firstPage.getMessage(), jobId,
                firstPage.getStatus(), firstPage.getProgressPercent(), firstPage.getElapsedMillis(), firstPage.getDurationMillis(),
                firstPage.getRowsAffected(), firstPage.getReturnedRowCount(), firstPage.getActualRowCount(), firstPage.getMaxVisibleRows(),
                firstPage.getMaxTotalRows(), firstPage.getHasResultSet(), pageIndex + context.resolvedBase(), pageSize, hasNext,
                truncated, note);
    }

    private SqlExecResult requestResultPageWithRetry(String jobId,
                                                     String sql,
                                                     int pageIndex,
                                                     int pageSize,
                                                     ResultFetchContext context,
                                                     AsyncJobStatus finalStatus) {
        int finalPageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);
        int attempts = Math.max(1, context.maxAttempts());
        int attempt = 0;
        long delay = Math.max(0, context.baseDelayMs());
        boolean useAlternateBase = false;
        RuntimeException lastError = null;
        while (attempt < attempts) {
            attempt++;
            int page = context.computePageNumber(pageIndex, useAlternateBase);
            try {
                JsonObject body = new JsonObject();
                body.addProperty("jobId", jobId);
                body.addProperty("removeAfterFetch", context.shouldUseRemoveAfterFetch(pageIndex));
                body.addProperty("page", page);
                body.addProperty("pageSize", finalPageSize);
                JsonObject resp = postJson("/jobs/result", body);
                boolean success = resp.has("success") && resp.get("success").getAsBoolean();
                Integer returnedRowCount = resp.has("returnedRowCount") && !resp.get("returnedRowCount").isJsonNull()
                        ? resp.get("returnedRowCount").getAsInt() : null;
                Integer actualRowCount = resp.has("actualRowCount") && !resp.get("actualRowCount").isJsonNull()
                        ? resp.get("actualRowCount").getAsInt() : null;
                Boolean hasResultSet = resp.has("hasResultSet") && !resp.get("hasResultSet").isJsonNull()
                        ? resp.get("hasResultSet").getAsBoolean() : null;
                String message = extractMessage(resp);
                logAttempt(jobId, attempt, pageIndex, page, finalPageSize, context, success, returnedRowCount, actualRowCount, hasResultSet, message, useAlternateBase);

                boolean transientUnavailable = isTransientUnavailable(message);
                SqlExecResult parsed = null;
                boolean contradictoryEmpty = false;
                if (success) {
                    parsed = parseResultResponse(resp, sql, jobId, page, pageSize);
                    contradictoryEmpty = isContradictoryEmpty(parsed, finalStatus, actualRowCount, returnedRowCount, hasResultSet);
                }

                if (!success && transientUnavailable) {
                    lastError = new RuntimeException(message != null ? message : "结果暂不可用");
                } else if (!success) {
                    throw new RuntimeException(message != null ? message : "结果已过期，请重新执行 SQL");
                }

                boolean shouldRetryForTransient = transientUnavailable;
                if (parsed != null && !contradictoryEmpty && !shouldRetryForTransient) {
                    context.resolveBase(page, pageIndex);
                    context.markResultFetched();
                    return parsed;
                }

                if (contradictoryEmpty || shouldRetryForTransient) {
                    if (context.canFallbackPageBase(useAlternateBase)) {
                        OperationLog.log("[" + jobId + "] 检测到结果为空/过期，将尝试切换 page 基准并重试");
                        useAlternateBase = true;
                    }
                    lastError = new RuntimeException(message != null ? message : "结果暂不可用");
                } else if (parsed == null) {
                    lastError = new RuntimeException(message != null ? message : "结果解析失败");
                }
            } catch (RuntimeException ex) {
                lastError = ex;
            }

            if (attempt >= attempts) {
                break;
            }
            long sleepMs = Math.min(context.maxDelayMs(), delay);
            OperationLog.log("[" + jobId + "] /jobs/result attempt=" + attempt + " 将在 " + sleepMs + "ms 后重试");
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("拉取结果被中断", ie);
            }
            delay = Math.min(context.maxDelayMs(), delay * 2);
        }
        throw lastError != null ? lastError : new RuntimeException("结果获取失败");
    }

    SqlExecResult parseResultResponse(JsonObject resp, String sql, String jobId, int requestedPage, int requestedPageSize) {
        try {
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
            Integer actualRowCount = resp.has("actualRowCount") && !resp.get("actualRowCount").isJsonNull()
                    ? resp.get("actualRowCount").getAsInt() : null;
            Integer maxVisibleRows = resp.has("maxVisibleRows") && !resp.get("maxVisibleRows").isJsonNull()
                    ? resp.get("maxVisibleRows").getAsInt() : null;
            Integer maxTotalRows = resp.has("maxTotalRows") && !resp.get("maxTotalRows").isJsonNull()
                    ? resp.get("maxTotalRows").getAsInt() : null;
            Boolean hasResultSet = resp.has("hasResultSet") && !resp.get("hasResultSet").isJsonNull()
                    ? resp.get("hasResultSet").getAsBoolean() : null;
            Boolean hasNext = resp.has("hasNext") && !resp.get("hasNext").isJsonNull()
                    ? resp.get("hasNext").getAsBoolean() : false;
            Boolean truncated = resp.has("truncated") && !resp.get("truncated").isJsonNull()
                    ? resp.get("truncated").getAsBoolean() : null;
            Integer respPage = resp.has("page") && !resp.get("page").isJsonNull()
                    ? resp.get("page").getAsInt() : requestedPage;
            Integer respPageSize = resp.has("pageSize") && !resp.get("pageSize").isJsonNull()
                    ? resp.get("pageSize").getAsInt() : requestedPageSize;
            String note = resp.has("note") && !resp.get("note").isJsonNull() ? resp.get("note").getAsString() : null;
            String message = extractMessage(resp);

            if (!success) {
                OperationLog.log("[" + jobId + "] 任务失败/未完成: " + message);
                throw new RuntimeException(message != null ? message : "结果已过期，请重新执行 SQL");
            }

            JsonArray rowsJson = extractResultRows(resp);
            List<String> columns = extractColumns(resp, rowsJson);
            List<java.util.Map<String, String>> rowMaps = extractRowMaps(rowsJson, columns);
            List<List<String>> rows = extractRows(rowMaps, columns);
            int rowCount = returnedRowCount != null ? returnedRowCount : rows.size();

            logResultDetails(jobId, respPage, respPageSize, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows,
                    hasNext, truncated, note, columns, rowsJson, rows);

            if (hasResultSet != null && !hasResultSet) {
                List<String> messageCols = List.of("消息");
                List<List<String>> messageRows = new ArrayList<>();
                String info = rowsAffected != null ? ("影响行数: " + rowsAffected) : (message != null ? message : "执行成功");
                messageRows.add(List.of(info));
                return new SqlExecResult(sql, messageCols, null, messageRows, List.of(), messageRows.size(), true, message, jobId, status, progress,
                        null, duration, rowsAffected, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows, hasResultSet,
                        respPage, respPageSize, hasNext, truncated, note);
            }

            return new SqlExecResult(sql, columns, null, rows, rowMaps, rowCount, true, message, jobId, status, progress,
                    null, duration, rowsAffected, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows, hasResultSet,
                    respPage, respPageSize, hasNext, truncated, note);
        } catch (Exception e) {
            OperationLog.log("[" + jobId + "] 解析 /jobs/result 失败: " + e.getMessage());
            throw new RuntimeException("解析结果失败: " + e.getMessage(), e);
        }
    }

    private int resolvePageSize(Integer desired) {
        int fallback = Config.getResultPageSizeOrDefault(DEFAULT_PAGE_SIZE);
        int target = desired == null ? fallback : desired;
        if (target < 1) {
            OperationLog.log("pageSize=" + target + " 无效，使用默认值 " + fallback);
            target = fallback;
        }
        if (target > MAX_PAGE_SIZE) {
            OperationLog.log("pageSize=" + target + " 超出上限，已裁剪到 " + MAX_PAGE_SIZE);
            target = MAX_PAGE_SIZE;
        }
        return target;
    }

    private int determineMaxRows(Integer explicitMaxRows, Integer pageSizeOverride) {
        int targetPageSize = resolvePageSize(pageSizeOverride);
        if (explicitMaxRows != null && explicitMaxRows > 0) {
            return Math.max(explicitMaxRows, targetPageSize);
        }
        return targetPageSize;
    }

    private JsonArray extractResultRows(JsonObject resp) {
        if (resp == null) {
            return new JsonArray();
        }
        if (resp.has("resultRows") && resp.get("resultRows").isJsonArray()) {
            return resp.getAsJsonArray("resultRows");
        }
        for (String key : List.of("rows", "data")) {
            if (resp.has(key) && resp.get(key).isJsonArray()) {
                return resp.getAsJsonArray(key);
            }
        }
        return new JsonArray();
    }

    private List<String> extractColumns(JsonObject resp, JsonArray rowsJson) {
        List<String> columns = new ArrayList<>();
        for (String key : List.of("columns", "resultColumns", "columnNames")) {
            if (resp != null && resp.has(key) && resp.get(key).isJsonArray()) {
                for (JsonElement el : resp.getAsJsonArray(key)) {
                    if (el != null && el.isJsonPrimitive()) {
                        columns.add(el.getAsString());
                    }
                }
                if (!columns.isEmpty()) {
                    return columns;
                }
            }
        }

        java.util.LinkedHashSet<String> mergedKeys = deriveColumnsFromObjects(rowsJson);
        if (!mergedKeys.isEmpty()) {
            return new ArrayList<>(mergedKeys);
        }

        if (rowsJson != null && rowsJson.size() > 0 && rowsJson.get(0).isJsonArray()) {
            int max = 0;
            for (JsonElement el : rowsJson) {
                if (el != null && el.isJsonArray()) {
                    max = Math.max(max, el.getAsJsonArray().size());
                }
            }
            for (int i = 0; i < max; i++) {
                columns.add("col_" + (i + 1));
            }
        }
        return columns;
    }

    private java.util.LinkedHashSet<String> deriveColumnsFromObjects(JsonArray rowsJson) {
        java.util.LinkedHashSet<String> keys = new java.util.LinkedHashSet<>();
        if (rowsJson == null) {
            return keys;
        }
        for (JsonElement el : rowsJson) {
            if (el != null && el.isJsonObject()) {
                for (Map.Entry<String, JsonElement> entry : el.getAsJsonObject().entrySet()) {
                    keys.add(entry.getKey());
                }
            }
        }
        return keys;
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
                    row.put(entry.getKey(), stringifyValue(entry.getValue()));
                }
                rows.add(row);
            } else if (el.isJsonArray()) {
                JsonArray arr = el.getAsJsonArray();
                java.util.Map<String, String> row = new java.util.LinkedHashMap<>();
                for (int i = 0; i < arr.size(); i++) {
                    String key = i < colCount ? columns.get(i) : ("col_" + (i + 1));
                    row.put(key, stringifyValue(arr.get(i)));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private List<List<String>> extractRows(List<java.util.Map<String, String>> rowMaps, List<String> columns) {
        List<List<String>> rows = new ArrayList<>();
        if (rowMaps == null) {
            return rows;
        }
        List<String> targetColumns = columns == null ? List.of() : columns;
        for (java.util.Map<String, String> map : rowMaps) {
            List<String> row = new ArrayList<>();
            for (String col : targetColumns) {
                row.add(map != null ? map.get(col) : null);
            }
            rows.add(row);
        }
        return rows;
    }

    private String stringifyValue(JsonElement v) {
        if (v == null || v.isJsonNull()) {
            return null;
        }
        if (v.isJsonPrimitive()) {
            return v.getAsString();
        }
        return gson.toJson(v);
    }

    private void logResultDetails(String jobId, Integer respPage, Integer respPageSize, Integer returnedRowCount,
                                  Integer actualRowCount, Integer maxVisibleRows, Integer maxTotalRows, Boolean hasNext,
                                  Boolean truncated, String note, List<String> columns, JsonArray rowsJson, List<List<String>> rows) {
        OperationLog.log("[" + jobId + "] 任务完成，page=" + respPage + " pageSize=" + respPageSize
                + "，returnedRowCount=" + (returnedRowCount != null ? returnedRowCount : rows.size())
                + (actualRowCount != null ? ("，actualRowCount=" + actualRowCount) : "")
                + (maxVisibleRows != null ? ("，maxVisibleRows=" + maxVisibleRows) : "")
                + (maxTotalRows != null ? ("，maxTotalRows=" + maxTotalRows) : "")
                + (hasNext != null && hasNext ? "，hasNext=true" : "")
                + (truncated != null && truncated ? "，truncated=true" : ""));
        OperationLog.log("[" + jobId + "] resultRows.size=" + (rowsJson == null ? 0 : rowsJson.size())
                + " columns=" + columns.size() + " -> " + abbreviate(String.join(",", columns)));
        if (rowsJson != null && rowsJson.size() > 0 && rowsJson.get(0).isJsonObject()) {
            var firstKeys = new ArrayList<String>();
            rowsJson.get(0).getAsJsonObject().keySet().forEach(firstKeys::add);
            OperationLog.log("[" + jobId + "] first row keys=" + abbreviate(String.join(",", firstKeys)));
        }
        if (note != null && !note.isBlank()) {
            OperationLog.log("[" + jobId + "] note: " + note);
        }
        if (Boolean.TRUE.equals(truncated)) {
            OperationLog.log("[" + jobId + "] 后端返回结果被截断（truncated=true）");
        }
    }

    private void logAttempt(String jobId, int attempt, int pageIndex, int page, int pageSize, ResultFetchContext context,
                            boolean success, Integer returnedRowCount, Integer actualRowCount, Boolean hasResultSet,
                            String message, boolean alternateBase) {
        OperationLog.log("[" + jobId + "] /jobs/result attempt=" + attempt
                + " page=" + page
                + " (base=" + context.describeBase(page, alternateBase) + ")"
                + " pageSize=" + pageSize
                + " removeAfterFetch=" + context.shouldUseRemoveAfterFetch(pageIndex)
                + " success=" + success
                + (hasResultSet != null ? (" hasResultSet=" + hasResultSet) : "")
                + (returnedRowCount != null ? (" returnedRowCount=" + returnedRowCount) : "")
                + (actualRowCount != null ? (" actualRowCount=" + actualRowCount) : "")
                + (message != null && !message.isBlank() ? (" message=" + abbreviate(message)) : ""));
    }

    private boolean isTransientUnavailable(String message) {
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        return lower.contains("result expired") || lower.contains("not available");
    }

    private boolean isContradictoryEmpty(SqlExecResult parsed,
                                         AsyncJobStatus finalStatus,
                                         Integer respActualRowCount,
                                         Integer returnedRowCount,
                                         Boolean respHasResultSet) {
        boolean hasResultSet = coalesceHasResultSet(parsed, finalStatus, respHasResultSet);
        if (!hasResultSet) {
            return false;
        }
        int expectedActual = firstPositive(respActualRowCount, parsed.getActualRowCount(),
                finalStatus != null ? finalStatus.getActualRowCount() : null,
                finalStatus != null ? finalStatus.getReturnedRowCount() : null,
                returnedRowCount);
        int returned = firstPositive(parsed.getReturnedRowCount(), returnedRowCount);
        int realRows = parsed.getRows() != null ? parsed.getRows().size() : 0;
        return expectedActual > 0 && (returned == 0 || realRows == 0);
    }

    private boolean coalesceHasResultSet(SqlExecResult parsed, AsyncJobStatus finalStatus, Boolean respHasResultSet) {
        if (parsed.getHasResultSet() != null) {
            return parsed.getHasResultSet();
        }
        if (respHasResultSet != null) {
            return respHasResultSet;
        }
        if (finalStatus != null && finalStatus.getHasResultSet() != null) {
            return finalStatus.getHasResultSet();
        }
        return true;
    }

    private int firstPositive(Integer... values) {
        if (values == null) {
            return 0;
        }
        for (Integer v : values) {
            if (v != null && v > 0) {
                return v;
            }
        }
        return 0;
    }

    private final class ResultFetchContext {
        private final AsyncResultConfig.PageBase configuredBase;
        private final int maxAttempts;
        private final long baseDelayMs;
        private final long maxDelayMs;
        private final AsyncResultConfig.RemoveAfterFetchStrategy removeAfterFetchStrategy;
        private Integer resolvedBase;
        private boolean fetched;

        ResultFetchContext(AsyncResultConfig.PageBase configuredBase,
                           int maxAttempts,
                           long baseDelayMs,
                           long maxDelayMs,
                           AsyncResultConfig.RemoveAfterFetchStrategy removeAfterFetchStrategy) {
            this.configuredBase = configuredBase == null ? AsyncResultConfig.PageBase.AUTO : configuredBase;
            this.maxAttempts = maxAttempts <= 0 ? 1 : maxAttempts;
            this.baseDelayMs = Math.max(0, baseDelayMs);
            this.maxDelayMs = Math.max(this.baseDelayMs, maxDelayMs);
            this.removeAfterFetchStrategy = removeAfterFetchStrategy == null
                    ? AsyncResultConfig.RemoveAfterFetchStrategy.AUTO
                    : removeAfterFetchStrategy;
        }

        int computePageNumber(int pageIndex, boolean useAlternateBase) {
            int base = resolvedBase != null ? resolvedBase : determineBase(useAlternateBase);
            return pageIndex + base;
        }

        int resolvedBase() {
            return resolvedBase != null ? resolvedBase : determineBase(false);
        }

        void resolveBase(int pageNumber, int pageIndex) {
            this.resolvedBase = Math.max(0, pageNumber - pageIndex);
        }

        boolean shouldUseRemoveAfterFetch(int pageIndex) {
            // 避免首次拉取后立即清理导致无法重试，统一改为在成功后追加清理请求
            return false;
        }

        boolean canFallbackPageBase(boolean alreadyAlternate) {
            return configuredBase == AsyncResultConfig.PageBase.AUTO && resolvedBase == null && !alreadyAlternate;
        }

        int maxAttempts() {
            return maxAttempts;
        }

        long baseDelayMs() {
            return baseDelayMs;
        }

        long maxDelayMs() {
            return maxDelayMs;
        }

        void markResultFetched() {
            this.fetched = true;
        }

        void cleanupResultIfNeeded(String jobId, int pageSize) {
            if (!fetched) {
                return;
            }
            if (removeAfterFetchStrategy == AsyncResultConfig.RemoveAfterFetchStrategy.FALSE) {
                return;
            }
            int page = computePageNumber(0, false);
            try {
                JsonObject body = new JsonObject();
                body.addProperty("jobId", jobId);
                body.addProperty("removeAfterFetch", true);
                body.addProperty("page", page);
                body.addProperty("pageSize", pageSize);
                postJson("/jobs/result", body);
                OperationLog.log("[" + jobId + "] 已使用 removeAfterFetch=true 清理结果缓存 (page=" + page + ")");
            } catch (Exception e) {
                OperationLog.log("[" + jobId + "] 清理结果缓存失败: " + e.getMessage());
            }
        }

        String describeBase(int page, boolean alternateBase) {
            int base = resolvedBase != null ? resolvedBase : determineBase(alternateBase);
            return base == 0 ? "0-based" : "1-based";
        }

        private int determineBase(boolean useAlternateBase) {
            int preferred = switch (configuredBase) {
                case ZERO_BASED -> 0;
                case ONE_BASED -> 1;
                default -> 1;
            };
            if (useAlternateBase) {
                return preferred == 0 ? 1 : 0;
            }
            return preferred;
        }
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
        Integer actualRowCount = obj.has("actualRowCount") && !obj.get("actualRowCount").isJsonNull()
                ? obj.get("actualRowCount").getAsInt() : null;
        Boolean hasResultSet = obj.has("hasResultSet") && !obj.get("hasResultSet").isJsonNull()
                ? obj.get("hasResultSet").getAsBoolean() : null;
        String message = extractMessage(obj);
        return new AsyncJobStatus(jobId, status, progress, elapsed, label, sqlSummary, rowsAffected, returnedRowCount, hasResultSet, actualRowCount, message);
    }

    private JsonObject postJson(String path, JsonObject payload) {
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
            Path path = writeHttpLogToFile(fullBody);
            String truncated = fullBody.substring(0, MAX_LOG_TEXT_LENGTH);
            sb.append(truncated);
            sb.append("\n<响应体超长，已写入: ").append(path.toAbsolutePath()).append('>');
        } else {
            sb.append(fullBody);
        }
        sb.append('\n').append("--- HTTP RESPONSE END ---");
        OperationLog.log(sb.toString());
    }

    private Path writeHttpLogToFile(String content) {
        try {
            String home = System.getProperty("user.home", "");
            Path dir = Path.of(home, ".Sql_client_v3", "logs");
            Files.createDirectories(dir);
            Path file = dir.resolve("http-" + LocalDate.now().toString().replaceAll("-", "") + ".log");
            Files.writeString(file, content + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            return file;
        } catch (Exception e) {
            OperationLog.log("写入 HTTP 日志文件失败: " + e.getMessage());
            return Path.of("http.log");
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
