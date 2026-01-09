package tools.sqlclient.remote;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.exec.AsyncJobStatus;
import tools.sqlclient.exec.ColumnMeta;
import tools.sqlclient.exec.ColumnNameNormalizer;
import tools.sqlclient.exec.DatabaseErrorInfo;
import tools.sqlclient.exec.OverloadedException;
import tools.sqlclient.exec.ResultExpiredException;
import tools.sqlclient.exec.ResultNotReadyException;
import tools.sqlclient.exec.ResultResponse;
import tools.sqlclient.exec.ThreadPoolSnapshot;
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
import java.util.UUID;

/**
 * 封装异步 SQL 服务调用，统一注入 Header、AES 加密与错误处理。
 */
public class RemoteSqlClient {
    private static final int MAX_LOG_TEXT_LENGTH = 200_000;
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Gson gson = new Gson();

    public AsyncJobStatus submitJob(String plainSql,
                                    Integer maxResultRows,
                                    String dbUser,
                                    String label) {
        JsonObject body = new JsonObject();
        body.addProperty("encryptedSql", AesCbc.encrypt(plainSql));
        if (maxResultRows != null && maxResultRows > 0) {
            body.addProperty("maxResultRows", maxResultRows);
        }
        if (dbUser != null && !dbUser.isBlank()) {
            body.addProperty("dbUser", dbUser);
        }
        if (label != null && !label.isBlank()) {
            body.addProperty("label", label);
        }
        JsonObject resp = postJson("/jobs/submit", body, false);
        AsyncJobStatus status = parseStatus(resp);
        if (Boolean.FALSE.equals(status.getSuccess()) || Boolean.TRUE.equals(status.getOverloaded())) {
            throw new OverloadedException(status.getMessage() != null ? status.getMessage() : "系统过载/提交失败");
        }
        OperationLog.log("[submit] jobId=" + status.getJobId()
                + " status=" + status.getStatus()
                + (status.getQueuedAt() != null ? (" queuedAt=" + status.getQueuedAt()) : "")
                + (status.getQueueDelayMillis() != null ? (" queueDelayMillis=" + status.getQueueDelayMillis()) : ""));
        return status;
    }

    public AsyncJobStatus pollStatus(String jobId) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        JsonObject resp = postJson("/jobs/status", body, false);
        AsyncJobStatus status = parseStatus(resp);
        if (status.getStatus() != null) {
            OperationLog.log("[" + jobId + "] status=" + status.getStatus()
                    + (status.getProgressPercent() != null ? (" progress=" + status.getProgressPercent() + "%") : "")
                    + (status.getQueueDelayMillis() != null ? (" queueDelayMillis=" + status.getQueueDelayMillis()) : ""));
        }
        return status;
    }

    public ResultResponse fetchResult(String jobId,
                                      Boolean removeAfterFetch,
                                      Integer page,
                                      Integer pageSize,
                                      Integer offset,
                                      Integer limit) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        if (removeAfterFetch != null) {
            body.addProperty("removeAfterFetch", removeAfterFetch);
        }
        String paginationNote = null;
        if (offset != null || limit != null) {
            int resolvedLimit = sanitizePageSize(limit);
            if (limit != null && limit > RemoteSqlConfig.SERVER_MAX_PAGE_SIZE) {
                paginationNote = "limit=" + limit + " 超过 1000，已自动裁剪为 " + resolvedLimit;
            }
            int resolvedOffset = offset != null && offset > 0 ? offset : 0;
            body.addProperty("offset", resolvedOffset);
            body.addProperty("limit", resolvedLimit);
        } else {
            int resolvedPageSize = sanitizePageSize(pageSize);
            if (pageSize != null && pageSize > RemoteSqlConfig.SERVER_MAX_PAGE_SIZE) {
                paginationNote = "pageSize=" + pageSize + " 超过 1000，已自动裁剪为 " + resolvedPageSize;
            }
            int resolvedPage = page != null && page > 0 ? page : 1;
            body.addProperty("page", resolvedPage);
            body.addProperty("pageSize", resolvedPageSize);
        }
        JsonObject resp = postJson("/jobs/result", body, true);
        ResultResponse result = parseResult(resp);
        if (paginationNote != null && (result.getNote() == null || result.getNote().isBlank())) {
            result.setNote(paginationNote);
        }
        if (Boolean.TRUE.equals(result.getOverloaded())) {
            throw new OverloadedException(result.getMessage() != null ? result.getMessage() : "系统过载");
        }
        if ("RESULT_EXPIRED".equalsIgnoreCase(result.getCode())) {
            throw new ResultExpiredException(jobId, result.getMessage() != null ? result.getMessage() : "结果已过期");
        }
        if ("RESULT_NOT_READY".equalsIgnoreCase(result.getCode())) {
            throw new ResultNotReadyException(jobId, result.getMessage() != null ? result.getMessage() : "结果未就绪");
        }
        return result;
    }

    public List<AsyncJobStatus> listActiveJobs() {
        JsonObject resp = postJson("/jobs/list", new JsonObject(), false);
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

    public AsyncJobStatus cancelJob(String jobId, String reason) {
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        if (reason != null && !reason.isBlank()) {
            body.addProperty("reason", reason);
        }
        JsonObject resp = postJson("/jobs/cancel", body, false);
        AsyncJobStatus status = parseStatus(resp);
        OperationLog.log("[" + jobId + "] 已发送取消请求，状态 " + status.getStatus());
        return status;
    }

    private int sanitizePageSize(Integer candidate) {
        int size = candidate != null && candidate > 0 ? candidate : 200;
        if (size > RemoteSqlConfig.SERVER_MAX_PAGE_SIZE) {
            size = RemoteSqlConfig.SERVER_MAX_PAGE_SIZE;
        }
        if (size < 1) {
            size = 1;
        }
        return size;
    }

    private JsonObject postJson(String path, JsonObject payload, boolean allow410) {
        try {
            String jsonBody = gson.toJson(payload);
            HttpRequest request = HttpRequest.newBuilder(RemoteSqlConfig.buildUri(path))
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .header("X-Request-Token", RemoteSqlConfig.REQUEST_TOKEN)
                    .timeout(Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();
            String requestId = UUID.randomUUID().toString();
            logHttpRequest(requestId, request, jsonBody);
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            int sc = resp.statusCode();
            logHttpResponse(requestId, request.uri().toString(), sc, resp.headers(), resp.body());
            if (allow410 && sc == 410) {
                JsonObject obj = safeParse(resp.body());
                if (!obj.has("code") || obj.get("code").isJsonNull()) {
                    obj.addProperty("code", "RESULT_EXPIRED");
                }
                if (!obj.has("status") || obj.get("status").isJsonNull()) {
                    obj.addProperty("status", "SUCCEEDED");
                }
                return obj;
            }
            if (sc == 401) {
                throw new RuntimeException("HTTP 401 未授权：请检查 X-Request-Token");
            }
            if (sc / 100 != 2) {
                throw new RuntimeException("HTTP " + sc + " | " + resp.body());
            }
            return safeParse(resp.body());
        } catch (ResultExpiredException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("请求失败: " + e.getMessage(), e);
        }
    }

    private JsonObject safeParse(String body) {
        try {
            JsonObject obj = gson.fromJson(body, JsonObject.class);
            return obj != null ? obj : new JsonObject();
        } catch (Exception e) {
            JsonObject obj = new JsonObject();
            obj.addProperty("message", "无法解析响应: " + e.getMessage());
            return obj;
        }
    }

    private AsyncJobStatus parseStatus(JsonObject obj) {
        String jobId = readString(obj, "jobId");
        Boolean success = readBoolean(obj, "success");
        String status = readString(obj, "status");
        Integer progress = readInt(obj, "progressPercent");
        Long elapsed = readLong(obj, "elapsedMillis");
        Long submittedAt = readLong(obj, "submittedAt");
        Long startedAt = readLong(obj, "startedAt");
        Long finishedAt = readLong(obj, "finishedAt");
        String label = readString(obj, "label");
        String sqlSummary = readString(obj, "sqlSummary");
        String dbUser = readString(obj, "dbUser");
        Integer rowsAffected = readInt(obj, "rowsAffected");
        Integer returnedRowCount = readInt(obj, "returnedRowCount");
        Integer actualRowCount = readInt(obj, "actualRowCount");
        Boolean hasResultSet = readBoolean(obj, "hasResultSet");
        Long queuedAt = readLong(obj, "queuedAt");
        Long queueDelayMillis = readLong(obj, "queueDelayMillis");
        Boolean overloaded = readBoolean(obj, "overloaded");
        ThreadPoolSnapshot threadPool = parseThreadPool(obj);
        String message = readString(obj, "message");
        String errorMessage = readString(obj, "errorMessage");
        if (message == null || message.isBlank()) {
            message = errorMessage;
        }
        Integer position = readInt(obj, "position");
        if (position == null) {
            position = readInt(obj, "Position");
        }
        DatabaseErrorInfo error = parseError(obj);
        if (error == null && (errorMessage != null || position != null)) {
            error = DatabaseErrorInfo.builder()
                    .message(errorMessage)
                    .position(position)
                    .build();
        }
        return new AsyncJobStatus(jobId, success, status, progress, elapsed, submittedAt, startedAt, finishedAt, label,
                sqlSummary, dbUser, rowsAffected, returnedRowCount, hasResultSet, actualRowCount, message, error, queuedAt,
                queueDelayMillis, overloaded, threadPool);
    }

    private ResultResponse parseResult(JsonObject obj) {
        ResultResponse rr = new ResultResponse();
        if (obj == null) {
            return rr;
        }
        rr.setJobId(readString(obj, "jobId"));
        rr.setSuccess(readBoolean(obj, "success"));
        rr.setStatus(readString(obj, "status"));
        rr.setCode(readString(obj, "code"));
        rr.setSubmittedAt(readLong(obj, "submittedAt"));
        Boolean resultAvailable = readBoolean(obj, "resultAvailable");
        rr.setResultAvailable(resultAvailable);
        rr.setArchived(readBoolean(obj, "archived"));
        rr.setArchiveStatus(readString(obj, "archiveStatus"));
        rr.setArchiveError(readString(obj, "archiveError"));
        rr.setArchivedAt(readLong(obj, "archivedAt"));
        rr.setExpiresAt(readLong(obj, "expiresAt"));
        rr.setLastAccessAt(readLong(obj, "lastAccessAt"));
        rr.setFinishedAt(readLong(obj, "finishedAt"));
        rr.setStartedAt(readLong(obj, "startedAt"));
        rr.setDbUser(readString(obj, "dbUser"));
        rr.setLabel(readString(obj, "label"));
        rr.setSqlSummary(readString(obj, "sqlSummary"));
        rr.setOverloaded(readBoolean(obj, "overloaded"));
        rr.setQueueDelayMillis(readLong(obj, "queueDelayMillis"));
        rr.setProgressPercent(readInt(obj, "progressPercent"));
        rr.setDurationMillis(readLong(obj, "durationMillis"));
        rr.setRowsAffected(readInt(obj, "rowsAffected"));
        rr.setQueuedAt(readLong(obj, "queuedAt"));
        rr.setOffset(readInt(obj, "offset"));
        rr.setLimit(readInt(obj, "limit"));
        rr.setReturnedRowCount(readInt(obj, "returnedRowCount"));
        rr.setActualRowCount(readInt(obj, "actualRowCount"));
        rr.setMaxVisibleRows(readInt(obj, "maxVisibleRows"));
        rr.setMaxTotalRows(readInt(obj, "maxTotalRows"));
        Boolean hasResultSet = readBoolean(obj, "hasResultSet");
        if (hasResultSet == null) {
            hasResultSet = resultAvailable;
        }
        rr.setHasResultSet(hasResultSet);
        rr.setIsSelect(readBoolean(obj, "isSelect"));
        rr.setResultType(readString(obj, "resultType"));
        rr.setUpdateCount(readInt(obj, "updateCount"));
        rr.setCommandTag(readString(obj, "commandTag"));
        rr.setPage(readInt(obj, "page"));
        rr.setPageSize(readInt(obj, "pageSize"));
        rr.setHasNext(readBoolean(obj, "hasNext"));
        rr.setTruncated(readBoolean(obj, "truncated"));
        rr.setError(parseError(obj));
        rr.setMessage(readString(obj, "message"));
        rr.setErrorMessage(readString(obj, "errorMessage"));
        Integer position = readInt(obj, "position");
        if (position == null) {
            position = readInt(obj, "Position");
        }
        rr.setPosition(position);
        rr.setNote(readString(obj, "note"));
        rr.setNotices(readStringList(obj, "notices"));
        rr.setWarnings(readStringList(obj, "warnings"));
        rr.setThreadPool(parseThreadPool(obj));
        List<ColumnMeta> columnMetas = extractColumnMetas(obj);
        List<String> rawColumns = extractColumns(obj);
        if (columnMetas != null && !columnMetas.isEmpty()) {
            rawColumns = columnMetas.stream()
                    .map(ColumnMeta::getName)
                    .toList();
        }
        List<String> expressions = extractColumnExpressions(obj);
        List<String> normalizedColumns = ColumnNameNormalizer.normalize(rawColumns, expressions);
        if (normalizedColumns.isEmpty() && rawColumns != null) {
            normalizedColumns = new ArrayList<>(rawColumns);
        }
        rr.setColumnMetas(columnMetas);
        rr.setColumns(normalizedColumns);
        rr.setColumnExpressions(expressions);
        JsonArray rows = extractRows(obj);
        rr.setRawRows(rows);
        rr.setRows(extractRowValues(rows));
        rr.setRowMaps(extractRowMaps(rows, normalizedColumns, rawColumns));
        rr.setResultRows(extractResultRows(rows));
        rr.setTotalRows(resolveTotalRows(obj));
        return rr;
    }

    private List<ColumnMeta> extractColumnMetas(JsonObject obj) {
        List<ColumnMeta> metas = new ArrayList<>();
        if (obj == null || !obj.has("columns") || !obj.get("columns").isJsonArray()) {
            return metas;
        }
        int idx = 0;
        for (JsonElement el : obj.getAsJsonArray("columns")) {
            idx++;
            if (el == null || !el.isJsonObject()) {
                continue;
            }
            JsonObject col = el.getAsJsonObject();
            ColumnMeta meta = new ColumnMeta();
            meta.setName(readString(col, "name"));
            String label = readString(col, "label");
            meta.setDisplayLabel(label != null ? label : readString(col, "displayLabel"));
            if (meta.getDisplayLabel() == null) {
                meta.setDisplayLabel(readString(col, "displayName"));
            }
            meta.setDataKey(readString(col, "dataKey"));
            meta.setDbType(readString(col, "dbType"));
            meta.setType(readString(col, "type"));
            Integer position = readInt(col, "position");
            if (position == null) {
                position = readInt(col, "index");
            }
            meta.setPosition(position != null ? position : idx);
            meta.setJdbcType(readInt(col, "jdbcType"));
            meta.setPrecision(readInt(col, "precision"));
            meta.setScale(readInt(col, "scale"));
            meta.setNullable(readInt(col, "nullable"));
            meta.setTableName(readString(col, "tableName"));
            meta.setSchemaName(readString(col, "schema"));
            if (meta.getDataKey() == null && meta.getName() != null) {
                meta.setDataKey(meta.getName());
            }
            if (meta.getDisplayLabel() == null && meta.getName() != null) {
                meta.setDisplayLabel(meta.getName());
            }
            metas.add(meta);
        }
        metas.sort((a, b) -> {
            Integer pa = a.getPosition();
            Integer pb = b.getPosition();
            if (pa == null && pb == null) return 0;
            if (pa == null) return 1;
            if (pb == null) return -1;
            return Integer.compare(pa, pb);
        });
        return metas;
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
        if (obj.has("columnDefs") && obj.get("columnDefs").isJsonArray()) {
            for (JsonElement el : obj.getAsJsonArray("columnDefs")) {
                if (el != null && el.isJsonObject()) {
                    JsonObject def = el.getAsJsonObject();
                    String name = readString(def, "name");
                    if (name == null) {
                        name = readString(def, "displayName");
                    }
                    if (name == null) {
                        name = readString(def, "column");
                    }
                    columns.add(name);
                }
            }
            if (!columns.isEmpty()) {
                return columns;
            }
        }
        return columns;
    }

    private List<String> extractColumnExpressions(JsonObject obj) {
        List<String> expressions = new ArrayList<>();
        if (obj == null) {
            return expressions;
        }
        for (String key : List.of("columnExprs", "selectList")) {
            if (obj.has(key) && obj.get(key).isJsonArray()) {
                for (JsonElement el : obj.getAsJsonArray(key)) {
                    if (el != null && el.isJsonPrimitive()) {
                        expressions.add(el.getAsString());
                    }
                }
                if (!expressions.isEmpty()) {
                    return expressions;
                }
            }
        }
        if (obj.has("columnDefs") && obj.get("columnDefs").isJsonArray()) {
            for (JsonElement el : obj.getAsJsonArray("columnDefs")) {
                if (el != null && el.isJsonObject()) {
                    JsonObject def = el.getAsJsonObject();
                    if (def.has("expr") && def.get("expr").isJsonPrimitive()) {
                        expressions.add(def.get("expr").getAsString());
                    } else {
                        expressions.add(null);
                    }
                }
            }
        }
        return expressions;
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

    private List<List<Object>> extractRowValues(JsonArray rowsJson) {
        List<List<Object>> rows = new ArrayList<>();
        if (rowsJson == null) {
            return rows;
        }
        for (JsonElement el : rowsJson) {
            if (el != null && el.isJsonArray()) {
                JsonArray arr = el.getAsJsonArray();
                List<Object> row = new ArrayList<>(arr.size());
                for (JsonElement cell : arr) {
                    row.add(toJavaObject(cell));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private List<Map<String, Object>> extractResultRows(JsonArray rowsJson) {
        List<Map<String, Object>> rows = new ArrayList<>();
        if (rowsJson == null) {
            return rows;
        }
        for (JsonElement el : rowsJson) {
            if (el != null && el.isJsonObject()) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                for (Map.Entry<String, JsonElement> entry : el.getAsJsonObject().entrySet()) {
                    row.put(entry.getKey(), toJavaObject(entry.getValue()));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private List<java.util.Map<String, String>> extractRowMaps(JsonArray rowsJson, List<String> columns, List<String> rawColumns) {
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

    private Object toJavaObject(JsonElement el) {
        if (el == null || el.isJsonNull()) {
            return null;
        }
        if (el.isJsonPrimitive()) {
            if (el.getAsJsonPrimitive().isBoolean()) {
                return el.getAsBoolean();
            }
            if (el.getAsJsonPrimitive().isNumber()) {
                return el.getAsNumber();
            }
            return el.getAsString();
        }
        if (el.isJsonArray()) {
            List<Object> list = new ArrayList<>();
            for (JsonElement child : el.getAsJsonArray()) {
                list.add(toJavaObject(child));
            }
            return list;
        }
        if (el.isJsonObject()) {
            Map<String, Object> map = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, JsonElement> entry : el.getAsJsonObject().entrySet()) {
                map.put(entry.getKey(), toJavaObject(entry.getValue()));
            }
            return map;
        }
        return gson.toJson(el);
    }

    private Integer resolveTotalRows(JsonObject obj) {
        if (obj == null) {
            return null;
        }
        for (String key : List.of("totalRows", "total", "totalCount", "returnedRowCount", "actualRowCount")) {
            Integer val = readInt(obj, key);
            if (val != null) {
                return val;
            }
        }
        return null;
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

    private String stringify(JsonElement v) {
        if (v == null || v.isJsonNull()) {
            return null;
        }
        if (v.isJsonPrimitive()) {
            return v.getAsString();
        }
        return gson.toJson(v);
    }

    private DatabaseErrorInfo parseError(JsonObject obj) {
        if (obj == null || !obj.has("error") || obj.get("error").isJsonNull()) {
            return null;
        }
        try {
            return DatabaseErrorInfo.fromJson(obj.getAsJsonObject("error"));
        } catch (Exception e) {
            return null;
        }
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

    private List<String> readStringList(JsonObject obj, String key) {
        if (obj == null || !obj.has(key) || obj.get(key).isJsonNull()) return null;
        try {
            JsonArray arr = obj.get(key).getAsJsonArray();
            List<String> list = new ArrayList<>();
            for (JsonElement el : arr) {
                list.add(el != null && !el.isJsonNull() ? el.getAsString() : null);
            }
            return list;
        } catch (Exception e) {
            return null;
        }
    }

    private void logHttpRequest(String requestId, HttpRequest request, String body) {
        if (!OperationLog.isReady()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("--- HTTP REQUEST BEGIN ---\n");
        sb.append("requestId: ").append(requestId).append('\n');
        sb.append(request.method()).append(' ').append(request.uri()).append('\n');
        sb.append("Headers:\n");
        request.headers().map().forEach((k, v) -> sb.append("  ").append(k).append(": ")
                .append(String.join(",", v)).append('\n'));
        sb.append("Body:\n");
        sb.append(body == null ? "<empty>" : body).append('\n');
        sb.append("--- HTTP REQUEST END ---");
        OperationLog.log(sb.toString());
    }

    private void logHttpResponse(String requestId, String url, int status, HttpHeaders headers, String body) {
        if (!OperationLog.isReady()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("--- HTTP RESPONSE BEGIN ---\n");
        sb.append("requestId: ").append(requestId).append('\n');
        sb.append("URL: ").append(url).append("\n");
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
