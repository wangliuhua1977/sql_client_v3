package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.db.LocalDatabasePathProvider;
import tools.sqlclient.db.SQLiteManager;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private SQLiteManager metadataReader;

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
        AsyncJobStatus submitted = submitJob(sql, maxResultRows, dbUser, null);
        try {
            AsyncJobStatus finalStatus = pollJobUntilDone(submitted.getJobId(), null).join();
            if (!"SUCCEEDED".equalsIgnoreCase(finalStatus.getStatus())) {
                throw new RuntimeException(finalStatus.getMessage() != null
                        ? finalStatus.getMessage()
                        : ("任务" + finalStatus.getJobId() + " 状态 " + finalStatus.getStatus()));
            }
            return requestResultPaged(submitted.getJobId(), sql, fetchAllPages, pageSizeOverride);
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

        return CompletableFuture.supplyAsync(() -> submitJob(trimmed, DEFAULT_MAX_RESULT_ROWS, dbUser, null), ThreadPools.NETWORK_POOL)
                .thenCompose(submit -> {
                    notifyStatus(onStatus, submit);
                    return pollJobUntilDone(submit.getJobId(), onStatus);
                })
                .thenCompose(status -> {
                    if ("SUCCEEDED".equalsIgnoreCase(status.getStatus())) {
                        return CompletableFuture.supplyAsync(() -> requestResultPaged(status.getJobId(), trimmed, false, pageSize), ThreadPools.NETWORK_POOL)
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
        return submitJob(sql, DEFAULT_MAX_RESULT_ROWS, null, null);
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
        int pageSize = resolvePageSize(pageSizeOverride);
        int currentPage = 1;
        boolean keepForPaging = Config.allowPagingAfterFirstFetch();
        boolean removeAfterFetch = fetchAllPages ? false : !keepForPaging;

        SqlExecResult firstPage = requestResultPage(jobId, sql, currentPage, pageSize, removeAfterFetch);
        if (!fetchAllPages || firstPage == null) {
            return firstPage;
        }

        List<List<String>> allRows = new ArrayList<>();
        if (firstPage.getRows() != null) {
            allRows.addAll(firstPage.getRows());
        }

        boolean hasNext = Boolean.TRUE.equals(firstPage.getHasNext());
        boolean truncated = Boolean.TRUE.equals(firstPage.getTruncated());
        String note = firstPage.getNote();
        boolean stoppedByLimit = false;

        while (hasNext && currentPage < MAX_ACCUMULATED_PAGES && allRows.size() < MAX_ACCUMULATED_ROWS) {
            currentPage++;
            boolean stopAfterThisPage = currentPage >= MAX_ACCUMULATED_PAGES || allRows.size() >= MAX_ACCUMULATED_ROWS;
            SqlExecResult pageResult = requestResultPage(jobId, sql, currentPage, pageSize, stopAfterThisPage);
            if (pageResult.getRows() != null) {
                allRows.addAll(pageResult.getRows());
            }
            if (pageResult.getTruncated() != null && pageResult.getTruncated()) {
                truncated = true;
            }
            if (pageResult.getNote() != null && !pageResult.getNote().isBlank()) {
                note = pageResult.getNote();
            }
            hasNext = Boolean.TRUE.equals(pageResult.getHasNext());
            if (stopAfterThisPage && hasNext) {
                stoppedByLimit = true;
                break;
            }
            if (allRows.size() >= MAX_ACCUMULATED_ROWS) {
                stoppedByLimit = true;
                break;
            }
        }

        if (stoppedByLimit && hasNext) {
            OperationLog.log("[" + jobId + "] 达到分页累积上限，已停止继续拉取剩余数据");
        }

        if (!removeAfterFetch && (!hasNext || stoppedByLimit)) {
            cleanupResult(jobId, currentPage, pageSize);
        }

        return new SqlExecResult(sql, firstPage.getColumns(), allRows, allRows.size(), true, firstPage.getMessage(), jobId,
                firstPage.getStatus(), firstPage.getProgressPercent(), firstPage.getElapsedMillis(), firstPage.getDurationMillis(),
                firstPage.getRowsAffected(), firstPage.getReturnedRowCount(), firstPage.getActualRowCount(), firstPage.getMaxVisibleRows(),
                firstPage.getMaxTotalRows(), firstPage.getHasResultSet(), currentPage, pageSize, hasNext,
                truncated, note);
    }

    private SqlExecResult requestResultPage(String jobId, String sql, int page, int pageSize, boolean removeAfterFetch) {
        int finalPageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);
        JsonObject body = new JsonObject();
        body.addProperty("jobId", jobId);
        body.addProperty("removeAfterFetch", removeAfterFetch);
        body.addProperty("page", page);
        body.addProperty("pageSize", finalPageSize);
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
                ? resp.get("page").getAsInt() : page;
        Integer respPageSize = resp.has("pageSize") && !resp.get("pageSize").isJsonNull()
                ? resp.get("pageSize").getAsInt() : pageSize;
        String note = resp.has("note") && !resp.get("note").isJsonNull() ? resp.get("note").getAsString() : null;
        String message = extractMessage(resp);

        if (!success) {
            OperationLog.log("[" + jobId + "] 任务失败/未完成: " + message);
            throw new RuntimeException(message != null ? message : "结果已过期，请重新执行 SQL");
        }

        if (hasResultSet != null && !hasResultSet) {
            List<String> columns = List.of("消息");
            List<List<String>> rows = new ArrayList<>();
            String info = rowsAffected != null ? ("影响行数: " + rowsAffected) : (message != null ? message : "执行成功");
            rows.add(List.of(info));
            return new SqlExecResult(sql, columns, rows, rows.size(), true, message, jobId, status, progress,
                    null, duration, rowsAffected, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows, hasResultSet, respPage, respPageSize, hasNext, truncated, note);
        }

        JsonArray rowsJson = resp.has("resultRows") && resp.get("resultRows").isJsonArray()
                ? resp.getAsJsonArray("resultRows") : new JsonArray();
        List<String> columns = extractColumns(resp, rowsJson, sql);
        if (columns.isEmpty()) {
            columns = deriveColumnsFromRows(rowsJson);
        }
        List<List<String>> rows = extractRows(rowsJson, columns);
        int rowCount = returnedRowCount != null ? returnedRowCount : rows.size();
        OperationLog.log("[" + jobId + "] 任务完成，page=" + respPage + " pageSize=" + respPageSize
                + "，returnedRowCount=" + (returnedRowCount != null ? returnedRowCount : rowCount)
                + (actualRowCount != null ? ("，actualRowCount=" + actualRowCount) : "")
                + (maxVisibleRows != null ? ("，maxVisibleRows=" + maxVisibleRows) : "")
                + (maxTotalRows != null ? ("，maxTotalRows=" + maxTotalRows) : "")
                + (hasNext ? "，hasNext=true" : "")
                + (truncated != null && truncated ? "，truncated=true" : ""));
        if (Boolean.TRUE.equals(truncated)) {
            OperationLog.log("[" + jobId + "] 后端返回结果被截断（truncated=true）");
        }
        if (note != null && !note.isBlank()) {
            OperationLog.log("[" + jobId + "] note: " + note);
        }
        return new SqlExecResult(sql, columns, rows, rowCount, true, message, jobId, status, progress,
                null, duration, rowsAffected, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows, hasResultSet, respPage, respPageSize, hasNext, truncated, note);
    }

    private void cleanupResult(String jobId, int page, int pageSize) {
        try {
            JsonObject body = new JsonObject();
            body.addProperty("jobId", jobId);
            body.addProperty("removeAfterFetch", true);
            body.addProperty("page", page);
            body.addProperty("pageSize", pageSize);
            postJson("/jobs/result", body);
            OperationLog.log("[" + jobId + "] 已清理后端结果缓存");
        } catch (Exception e) {
            OperationLog.log("[" + jobId + "] 清理结果缓存失败: " + e.getMessage());
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

    private List<String> extractColumns(JsonObject resp, JsonArray rowsJson, String sql) {
        List<String> columns = new ArrayList<>();
        for (String key : List.of("columns", "resultColumns", "columnNames")) {
            if (resp.has(key) && resp.get(key).isJsonArray()) {
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

        JsonElement first = rowsJson.size() > 0 ? rowsJson.get(0) : null;
        if (first != null && first.isJsonObject()) {
            List<String> ordered = resolveColumnsFromSql(sql, first.getAsJsonObject());
            if (ordered != null && !ordered.isEmpty()) {
                return ordered;
            }
            // JsonObject 在 Gson 中保持插入顺序，避免依赖 HashMap
            for (Map.Entry<String, JsonElement> entry : first.getAsJsonObject().entrySet()) {
                columns.add(entry.getKey());
            }
        } else if (first != null && first.isJsonArray()) {
            JsonArray arr = first.getAsJsonArray();
            for (int i = 0; i < arr.size(); i++) {
                columns.add("col_" + (i + 1));
            }
        }
        return columns;
    }

    private List<String> deriveColumnsFromRows(JsonArray rowsJson) {
        if (rowsJson == null || rowsJson.size() == 0) {
            return new ArrayList<>();
        }
        JsonElement first = rowsJson.get(0);
        if (first != null && first.isJsonObject()) {
            List<String> keys = new ArrayList<>();
            for (Map.Entry<String, JsonElement> entry : first.getAsJsonObject().entrySet()) {
                keys.add(entry.getKey());
            }
            return keys;
        }
        int max = 0;
        for (JsonElement el : rowsJson) {
            if (el != null && el.isJsonArray()) {
                max = Math.max(max, el.getAsJsonArray().size());
            }
        }
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            columns.add("col" + (i + 1));
        }
        return columns;
    }

    private List<List<String>> extractRows(JsonArray rowsJson, List<String> columns) {
        List<List<String>> rows = new ArrayList<>();
        for (JsonElement el : rowsJson) {
            if (el == null || el.isJsonNull()) {
                continue;
            }
            if (el.isJsonArray()) {
                JsonArray arr = el.getAsJsonArray();
                List<String> row = new ArrayList<>();
                for (int i = 0; i < arr.size(); i++) {
                    JsonElement v = arr.get(i);
                    row.add(v == null || v.isJsonNull() ? "" : v.getAsString());
                }
                rows.add(row);
            } else if (el.isJsonObject()) {
                JsonObject obj = el.getAsJsonObject();
                List<String> row = new ArrayList<>();
                for (String col : columns) {
                    JsonElement v = obj.get(col);
                    row.add(v == null || v.isJsonNull() ? "" : v.getAsString());
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private List<String> resolveColumnsFromSql(String sql, JsonObject firstRow) {
        String normalized = normalizeSql(sql);
        if (normalized.isEmpty()) {
            return null;
        }
        List<String> star = tryResolveStarColumns(normalized);
        if (star != null && !star.isEmpty()) {
            return star;
        }
        if (firstRow == null) {
            return null;
        }
        List<String> explicit = tryResolveExplicitColumns(normalized);
        return explicit != null && !explicit.isEmpty() ? explicit : null;
    }

    private List<String> tryResolveStarColumns(String sql) {
        String compact = sql.replaceAll("\\s+", " ").trim();
        Pattern pattern = Pattern.compile("^select \\\* from ([A-Za-z0-9_\\.]+)\\s*;?$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(compact);
        if (!matcher.matches()) {
            return null;
        }
        if (containsComplexKeyword(compact)) {
            return null;
        }
        String tableToken = matcher.group(1);
        String objectName = tableToken.contains(".")
                ? tableToken.substring(tableToken.lastIndexOf('.') + 1)
                : tableToken;
        return loadCachedColumns(objectName);
    }

    private boolean containsComplexKeyword(String sql) {
        Pattern forbidden = Pattern.compile("\\b(join|where|group\\s+by|order\\s+by|limit|union|with|having)\\b",
                Pattern.CASE_INSENSITIVE);
        return forbidden.matcher(sql).find();
    }

    private List<String> tryResolveExplicitColumns(String sql) {
        Matcher selectMatcher = Pattern.compile("\\bselect\\b", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (!selectMatcher.find()) {
            return null;
        }
        Matcher fromMatcher = Pattern.compile("\\bfrom\\b", Pattern.CASE_INSENSITIVE).matcher(sql);
        if (!fromMatcher.find(selectMatcher.end())) {
            return null;
        }
        int fromIdx = fromMatcher.start();
        String selectPart = sql.substring(selectMatcher.end(), fromIdx).trim();
        if (selectPart.toLowerCase(Locale.ROOT).startsWith("distinct ")) {
            selectPart = selectPart.substring("distinct".length()).trim();
        }
        if (selectPart.isEmpty()) {
            return null;
        }
        List<String> tokens = splitColumns(selectPart);
        if (tokens.isEmpty()) {
            return null;
        }
        List<String> columns = new ArrayList<>();
        for (String token : tokens) {
            String trimmed = token.trim();
            if (isComplexToken(trimmed)) {
                return null;
            }
            String alias = extractAlias(trimmed);
            if (alias != null && !alias.isBlank()) {
                columns.add(alias);
                continue;
            }
            String stripped = stripQualifier(trimmed);
            columns.add(stripped);
        }
        return columns;
    }

    private List<String> splitColumns(String selectPart) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inQuote = false;
        for (int i = 0; i < selectPart.length(); i++) {
            char ch = selectPart.charAt(i);
            if (ch == '\'' || ch == '"') {
                inQuote = !inQuote;
            }
            if (!inQuote) {
                if (ch == '(') depth++;
                if (ch == ')') depth = Math.max(0, depth - 1);
                if (ch == ',' && depth == 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(ch);
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private boolean isComplexToken(String token) {
        String lower = token.toLowerCase(Locale.ROOT);
        if (lower.contains(" select ") || lower.contains(" case ")) {
            return true;
        }
        for (char c : new char[]{'(', ')', '+', '/', '-'}) {
            if (token.indexOf(c) >= 0) {
                return true;
            }
        }
        int starIdx = token.indexOf('*');
        return starIdx > 0 || (lower.contains("*") && token.trim().length() != 1);
    }

    private String extractAlias(String token) {
        String lower = token.toLowerCase(Locale.ROOT);
        int asIdx = lower.lastIndexOf(" as ");
        if (asIdx >= 0) {
            return stripQuotes(token.substring(asIdx + 4).trim());
        }
        String[] parts = token.trim().split("\\s+");
        if (parts.length >= 2) {
            return stripQuotes(parts[parts.length - 1]);
        }
        return null;
    }

    private String stripQualifier(String token) {
        String stripped = token.trim();
        int dot = stripped.lastIndexOf('.');
        if (dot >= 0 && dot < stripped.length() - 1) {
            stripped = stripped.substring(dot + 1);
        }
        return stripQuotes(stripped);
    }

    private String stripQuotes(String text) {
        if (text == null || text.isBlank()) {
            return text;
        }
        String trimmed = text.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || (trimmed.startsWith("`") && trimmed.endsWith("`"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private List<String> loadCachedColumns(String objectName) {
        String name = sanitizeObjectName(objectName);
        if (name.isEmpty()) {
            return List.of();
        }
        List<String> cols = new ArrayList<>();
        try (Connection conn = metadataDb().getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT column_name FROM columns WHERE object_name=? ORDER BY sort_no")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(rs.getString(1));
                }
            }
        } catch (Exception e) {
            OperationLog.log("读取本地列缓存失败: " + e.getMessage());
        }
        return cols;
    }

    private String sanitizeObjectName(String objectName) {
        if (objectName == null) {
            return "";
        }
        String trimmed = objectName.trim();
        if (trimmed.contains(".")) {
            trimmed = trimmed.substring(trimmed.lastIndexOf('.') + 1);
        }
        return trimmed.replace("'", "");
    }

    private SQLiteManager metadataDb() {
        if (metadataReader == null) {
            metadataReader = new SQLiteManager(LocalDatabasePathProvider.resolveMetadataDbPath());
            metadataReader.initSchema();
        }
        return metadataReader;
    }

    private String normalizeSql(String sql) {
        if (sql == null) {
            return "";
        }
        String withoutBlock = sql.replaceAll("(?s)/\\*.*?\\*/", " ");
        String withoutLine = withoutBlock.replaceAll("(?m)^\\s*--.*?$", " ");
        return withoutLine.trim();
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
