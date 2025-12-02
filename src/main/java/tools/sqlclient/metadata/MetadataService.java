package tools.sqlclient.metadata;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.network.TrustAllHttpClient;
import tools.sqlclient.util.OperationLog;

import javax.swing.*;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * 管理元数据：差分更新、本地缓存、模糊匹配。
 */
public class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    private static final String OBJ_API = "https://leshan.paas.sc.ctc.com/waf-dev/api?api_id=sql_obj&api_token=jtc3r&p_type=1";
    private static final String COL_API_TEMPLATE = "https://leshan.paas.sc.ctc.com/waf-dev/api?api_id=sql_obj&api_token=jtc3r&p_type=2&p_obj=%s";

    private final SQLiteManager sqliteManager;
    private final ExecutorService metadataPool = Executors.newFixedThreadPool(4, r -> new Thread(r, "metadata-pool"));
    private final ExecutorService networkPool = Executors.newFixedThreadPool(6, r -> new Thread(r, "network-pool"));
    private final Gson gson = new Gson();
    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Object dbWriteLock = new Object();
    private final Set<String> inflightColumns = java.util.Collections.synchronizedSet(new HashSet<>());

    public record MetadataRefreshResult(boolean success, int totalObjects, int changedObjects, String message) {}

    public MetadataService(Path dbPath) {
        this.sqliteManager = new SQLiteManager(dbPath);
        this.sqliteManager.initSchema();
    }

    public void refreshMetadataAsync(Runnable done) {
        refreshMetadataAsync(result -> {
            if (done != null) {
                done.run();
            }
        });
    }

    public void refreshMetadataAsync(java.util.function.Consumer<MetadataRefreshResult> done) {
        CompletableFuture
                .supplyAsync(this::refreshMetadata, metadataPool)
                .handle((result, ex) -> {
                    if (ex != null) {
                        log.error("刷新元数据失败", ex);
                        return new MetadataRefreshResult(false, countCachedObjects(), 0, ex.getMessage());
                    }
                    return result;
                })
                .whenComplete((result, ex) -> {
                    if (done != null) {
                        SwingUtilities.invokeLater(() -> done.accept(result));
                    }
                });
    }

    /**
     * 清空本地对象/字段缓存并重新拉取一次对象列表。
     */
    public void resetMetadataAsync(Runnable done) {
        resetMetadataAsync(result -> {
            if (done != null) {
                done.run();
            }
        });
    }

    public void resetMetadataAsync(java.util.function.Consumer<MetadataRefreshResult> done) {
        CompletableFuture.supplyAsync(() -> {
            clearLocalMetadata();
            return refreshMetadata();
        }, metadataPool).handle((result, ex) -> {
            if (ex != null) {
                log.error("重置元数据失败", ex);
                return new MetadataRefreshResult(false, countCachedObjects(), 0, ex.getMessage());
            }
            return result;
        }).whenComplete((result, ex) -> {
            if (done != null) {
                SwingUtilities.invokeLater(() -> done.accept(result));
            }
        });
    }

    private MetadataRefreshResult refreshMetadata() {
        try {
            OperationLog.log("开始刷新对象元数据");
            List<RemoteObject> remoteObjects = fetchObjects();
            Set<String> remoteNames = remoteObjects.stream().map(o -> o.object_name).collect(Collectors.toSet());

            int added = 0;
            int removed = 0;

            synchronized (dbWriteLock) {
                try (Connection conn = sqliteManager.getConnection()) {
                    conn.setAutoCommit(false);
                    Set<String> existing = new HashSet<>();
                    try (PreparedStatement st = conn.prepareStatement("SELECT object_name FROM objects")) {
                        try (ResultSet rs = st.executeQuery()) {
                            while (rs.next()) {
                                existing.add(rs.getString(1));
                            }
                        }
                    }
                    // 插入新增
                    try (PreparedStatement ps = conn.prepareStatement("INSERT OR IGNORE INTO objects(schema_name, object_name, object_type) VALUES(?,?,?)")) {
                        for (RemoteObject obj : remoteObjects) {
                            ps.setString(1, obj.schema_name);
                            ps.setString(2, obj.object_name);
                            ps.setString(3, obj.object_type);
                            ps.addBatch();
                            if (!existing.contains(obj.object_name)) {
                                added++;
                            }
                        }
                        ps.executeBatch();
                    }
                    // 删除不存在（可扩展: 通过配置控制，这里默认删除）
                    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM objects WHERE object_name = ?")) {
                        for (String old : existing) {
                            if (!remoteNames.contains(old)) {
                                ps.setString(1, old);
                                ps.addBatch();
                                removed++;
                            }
                        }
                        ps.executeBatch();
                    }
                    conn.commit();
                }
            }

            // 更新列信息
            OperationLog.log("对象同步完成，共 " + remoteObjects.size() + " 个");
            return new MetadataRefreshResult(true, countCachedObjects(), added + removed, "刷新成功");
        } catch (Exception e) {
            log.error("刷新元数据失败", e);
            OperationLog.log("刷新元数据失败: " + e.getMessage());
            return new MetadataRefreshResult(false, countCachedObjects(), 0, e.getMessage());
        }
    }

    private void clearLocalMetadata() {
        synchronized (dbWriteLock) {
            try (Connection conn = sqliteManager.getConnection(); Statement st = conn.createStatement()) {
                boolean auto = conn.getAutoCommit();
                if (auto) conn.setAutoCommit(false);
                st.executeUpdate("DELETE FROM columns");
                st.executeUpdate("DELETE FROM objects");
                conn.commit();
                if (auto) conn.setAutoCommit(true);
                OperationLog.log("已清空本地元数据");
            } catch (Exception e) {
                log.error("清空本地元数据失败", e);
                OperationLog.log("清空本地元数据失败: " + e.getMessage());
            }
        }
    }

    public int countCachedObjects() {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(1) FROM objects")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Exception e) {
            log.warn("统计本地对象数量失败", e);
        }
        return 0;
    }

    /**
     * 在用户选择表名或输入别名时，后台补齐字段元数据，避免列联想时出现空列表。
     */
    /**
     * 异步补齐字段缓存，可在完成后回调刷新 UI。
     * @param tableName 目标表/视图
     * @param onFreshLoaded 当本次确实发起远程抓取且成功完成时的回调（UI 线程）
     * @return 是否发起了新的抓取
     */
    public boolean ensureColumnsCachedAsync(String tableName, Runnable onFreshLoaded) {
        return ensureColumnsCachedAsync(tableName, false, onFreshLoaded);
    }

    public boolean ensureColumnsCachedAsync(String tableName, boolean forceRefresh) {
        return ensureColumnsCachedAsync(tableName, forceRefresh, null);
    }

    public boolean ensureColumnsCachedAsync(String tableName, boolean forceRefresh, Runnable onFreshLoaded) {
        if (!shouldFetchColumns(tableName, forceRefresh)) return false;
        String key = tableName + (forceRefresh ? "#force" : "");
        if (!inflightColumns.add(key)) return false;
        CompletableFuture.runAsync(() -> updateColumns(tableName, forceRefresh), networkPool)
                .whenComplete((v, ex) -> {
                    inflightColumns.remove(key);
                    if (onFreshLoaded != null) {
                        SwingUtilities.invokeLater(onFreshLoaded);
                    }
                });
        return true;
    }

    public boolean ensureColumnsCachedAsync(String tableName) {
        return ensureColumnsCachedAsync(tableName, false, null);
    }

    private boolean shouldFetchColumns(String tableName, boolean forceRefresh) {
        if (tableName == null || tableName.isBlank()) return false;
        if (!isLikelyTableName(tableName)) return false;
        if (!forceRefresh && hasColumns(tableName)) return false;
        return true;
    }

    private boolean hasColumns(String tableName) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(1) FROM columns WHERE object_name=?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (Exception e) {
            log.warn("检查列缓存失败: {}", tableName, e);
            return false;
        }
    }

    private boolean isLikelyTableName(String name) {
        if (name == null) return false;
        String trimmed = name.trim();
        if (trimmed.isEmpty()) return false;
        if (trimmed.contains(" ") || trimmed.contains("\n")) return false;
        return trimmed.matches("[A-Za-z0-9_\\.]{2,128}");
    }

    private List<RemoteObject> fetchObjects() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(OBJ_API)).GET().build();
        HttpResponse<java.io.InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        OperationLog.log("对象列表请求: " + request.uri());
        Type listType = new TypeToken<List<RemoteObject>>(){}.getType();
        try (InputStreamReader reader = new InputStreamReader(response.body(), StandardCharsets.UTF_8)) {
            JsonElement root = gson.fromJson(reader, JsonElement.class);
            OperationLog.log("对象列表响应: " + OperationLog.abbreviate(String.valueOf(root), 300));
            return parseListFromJson(root, listType);
        }
    }

    private void updateColumns(String objectName, boolean forceRefresh) {
        boolean knownTableOrView = isTableOrView(objectName);
        int attempts = 0;
        while (attempts < 3) {
            try {
                attempts++;
                String encodedName = URLEncoder.encode(objectName, StandardCharsets.UTF_8);
                HttpRequest request = HttpRequest.newBuilder(URI.create(COL_API_TEMPLATE.formatted(encodedName))).GET().build();
                OperationLog.log("字段列表请求: " + request.uri());
                HttpResponse<java.io.InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
                Type listType = new TypeToken<List<RemoteColumn>>(){}.getType();
                List<RemoteColumn> columns;
                try (InputStreamReader reader = new InputStreamReader(response.body(), StandardCharsets.UTF_8)) {
                    JsonElement root = gson.fromJson(reader, JsonElement.class);
                    OperationLog.log("字段列表响应: " + OperationLog.abbreviate(String.valueOf(root), 300));
                    columns = parseListFromJson(root, listType);
                }
                if (columns == null || columns.isEmpty()) return;
                synchronized (dbWriteLock) {
                    try (Connection conn = sqliteManager.getConnection()) {
                        boolean auto = conn.getAutoCommit();
                        if (auto) {
                            conn.setAutoCommit(false);
                        }
                        java.sql.Savepoint sp = conn.setSavepoint("col_upd");
                        try {
                            if (!knownTableOrView) {
                                try (PreparedStatement up = conn.prepareStatement(
                                        "INSERT OR IGNORE INTO objects(schema_name, object_name, object_type, use_count, last_used_at) VALUES(?,?,?,?,?)")) {
                                    up.setString(1, columns.get(0).schema_name);
                                    up.setString(2, objectName);
                                    up.setString(3, "table");
                                    up.setInt(4, 0);
                                    up.setLong(5, System.currentTimeMillis());
                                    up.executeUpdate();
                                }
                            }
                            Map<String, Integer> localCols = new HashMap<>();
                            try (PreparedStatement ps = conn.prepareStatement("SELECT column_name, sort_no FROM columns WHERE object_name=?")) {
                                ps.setString(1, objectName);
                                try (ResultSet rs = ps.executeQuery()) {
                                    while (rs.next()) {
                                        localCols.put(rs.getString("column_name"), rs.getInt("sort_no"));
                                    }
                                }
                            }
                            boolean changed = forceRefresh || columns.size() != localCols.size();
                            if (!changed) {
                                for (RemoteColumn col : columns) {
                                    Integer localSort = localCols.get(col.column_name);
                                    int remoteSort = parseSortNo(col.sort_no, -1);
                                    if (localSort == null || (remoteSort >= 0 && localSort != remoteSort)) {
                                        changed = true;
                                        break;
                                    }
                                }
                            }
                            if (changed) {
                                try (PreparedStatement del = conn.prepareStatement("DELETE FROM columns WHERE object_name=?")) {
                                    del.setString(1, objectName);
                                    del.executeUpdate();
                                }
                                try (PreparedStatement ins = conn.prepareStatement("INSERT INTO columns(schema_name, object_name, column_name, sort_no) VALUES(?,?,?,?)")) {
                                    int i = 0;
                                    for (RemoteColumn col : columns) {
                                        ins.setString(1, col.schema_name);
                                        ins.setString(2, objectName);
                                        ins.setString(3, col.column_name);
                                        int sort = parseSortNo(col.sort_no, i++);
                                        ins.setInt(4, sort);
                                        ins.addBatch();
                                    }
                                    ins.executeBatch();
                                }
                            }
                            conn.releaseSavepoint(sp);
                            conn.commit();
                        } catch (Exception ex) {
                            try { conn.rollback(sp); } catch (Exception ignore) {}
                            throw ex;
                        } finally {
                            try { if (auto) conn.setAutoCommit(true); } catch (Exception ignore) {}
                        }
                    }
                }
                return;
            } catch (Exception e) {
                if (attempts >= 3) {
                    log.error("更新字段失败: {}", objectName, e);
                    OperationLog.log("更新字段失败: " + objectName + " | " + e.getMessage());
                } else {
                    try { Thread.sleep(300L * attempts); } catch (InterruptedException ignored) {}
                }
            }
        }
    }

    private boolean isTableOrView(String objectName) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT object_type FROM objects WHERE object_name=?")) {
            ps.setString(1, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String type = rs.getString(1);
                    return "table".equalsIgnoreCase(type) || "view".equalsIgnoreCase(type);
                }
            }
        } catch (Exception e) {
            log.warn("校验对象类型失败: {}", objectName, e);
        }
        return false;
    }

    /**
     * 仅检查本地缓存中是否存在 table/view 记录，不触发网络请求。
     */
    public boolean isKnownTableOrViewCached(String objectName) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM objects WHERE object_name=? AND object_type IN ('table','view')")) {
            ps.setString(1, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            log.warn("检查缓存对象失败: {}", objectName, e);
            return false;
        }
    }

    public List<SuggestionItem> suggest(String token, SuggestionContext context, int limit) {
        String likePattern = toLikePattern(token);
        if (likePattern == null) return List.of();
        return switch (context.type()) {
            case TABLE_OR_VIEW -> queryObjects(likePattern, Set.of("table", "view"), limit);
            case FUNCTION -> queryObjects(likePattern, Set.of("function"), limit);
            case PROCEDURE -> queryObjects(likePattern, Set.of("procedure"), limit);
            case COLUMN -> queryColumns(likePattern, context.tableHint(), context.scopedTables(), limit);
        };
    }

    private String toLikePattern(String token) {
        if (token == null) return null;
        if (token.isBlank()) return "%";
        String[] keywords = token.toLowerCase().split("%");
        return Arrays.stream(keywords).map(k -> "%" + k + "%").collect(Collectors.joining());
    }

    private List<SuggestionItem> queryObjects(String likeSql, Set<String> types, int limit) {
        String placeholders = types.stream().map(t -> "?").collect(Collectors.joining(","));
        String sql = "SELECT object_name, object_type, use_count FROM objects WHERE object_type IN (" + placeholders + ") " +
                "AND lower(object_name) LIKE ? ORDER BY use_count DESC, object_name ASC LIMIT ?";
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String type : types) {
                ps.setString(idx++, type);
            }
            ps.setString(idx++, likeSql);
            ps.setInt(idx, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<SuggestionItem> items = new ArrayList<>();
                while (rs.next()) {
                    items.add(new SuggestionItem(rs.getString("object_name"), rs.getString("object_type"), null,
                            rs.getInt("use_count")));
                }
                return items;
            }
        } catch (Exception e) {
            log.error("模糊匹配失败", e);
            return List.of();
        }
    }

    private List<SuggestionItem> queryColumns(String likeSql, String tableHint, List<String> scopedTables, int limit) {
        StringBuilder sql = new StringBuilder("SELECT column_name, object_name, sort_no, use_count FROM columns WHERE lower(column_name) LIKE ?");
        List<String> tables = scopedTables == null ? List.of() : scopedTables.stream().filter(Objects::nonNull).distinct().toList();
        if (tableHint != null && !tableHint.isBlank()) {
            sql.append(" AND object_name = ?");
        } else if (!tables.isEmpty()) {
            sql.append(" AND object_name IN (" + tables.stream().map(t -> "?").collect(Collectors.joining(",")) + ")");
        }
        sql.append(" ORDER BY sort_no ASC, use_count DESC, column_name ASC LIMIT ?");
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setString(idx++, likeSql);
            if (tableHint != null && !tableHint.isBlank()) {
                ps.setString(idx++, tableHint);
            } else {
                for (String t : tables) {
                    ps.setString(idx++, t);
                }
            }
            ps.setInt(idx, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<SuggestionItem> items = new ArrayList<>();
                while (rs.next()) {
                    items.add(new SuggestionItem(rs.getString("column_name"), "column", rs.getString("object_name"),
                            rs.getInt("use_count")));
                }
                return items;
            }
        } catch (Exception e) {
            log.error("列模糊匹配失败", e);
            return List.of();
        }
    }

    /**
     * 提供对象浏览器使用的表/视图 + 字段列表查询。
     */
    public List<TableEntry> listTables(String keyword) {
        String like = (keyword == null || keyword.isBlank()) ? "%" : toLikePattern(keyword);
        if (like == null) like = "%";
        String sql = "SELECT object_name, object_type FROM objects WHERE object_type IN ('table','view')" +
                ("%".equals(like) ? "" : " AND lower(object_name) LIKE ?") + " ORDER BY object_name";
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (!"%".equals(like)) {
                ps.setString(1, like);
            }
            List<TableEntry> list = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("object_name");
                    String type = rs.getString("object_type");
                    list.add(new TableEntry(name, type));
                }
            }
            return list;
        } catch (Exception e) {
            log.error("查询对象浏览器数据失败", e);
            return List.of();
        }
    }

    public List<String> loadColumnsFromCache(String tableName) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT column_name FROM columns WHERE object_name=? ORDER BY sort_no ASC, column_name ASC")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> cols = new ArrayList<>();
                while (rs.next()) {
                    cols.add(rs.getString(1));
                }
                return cols;
            }
        } catch (Exception e) {
            log.warn("读取列失败: {}", tableName, e);
            return List.of();
        }
    }

    public void recordUsage(SuggestionItem item) {
        long now = Instant.now().toEpochMilli();
        synchronized (dbWriteLock) {
            try (Connection conn = sqliteManager.getConnection()) {
                if ("column".equals(item.type())) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE columns SET use_count = use_count + 1, last_used_at=? WHERE object_name=? AND column_name=?")) {
                        ps.setLong(1, now);
                        ps.setString(2, item.tableHint());
                        ps.setString(3, item.name());
                        ps.executeUpdate();
                    }
                } else {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE objects SET use_count = use_count + 1, last_used_at=? WHERE object_name=?")) {
                        ps.setLong(1, now);
                        ps.setString(2, item.name());
                        ps.executeUpdate();
                    }
                }
            } catch (Exception e) {
                log.warn("更新使用次数失败: {}", item.name(), e);
            }
        }
    }

    /**
     * 兼容 API 返回值既可能是数组，也可能是包装对象（如 {"data": [...]}）。
     */
    private <T> List<T> parseListFromJson(JsonElement root, Type listType) {
        if (root == null || root.isJsonNull()) {
            return List.of();
        }
        if (root.isJsonArray()) {
            return gson.fromJson(root, listType);
        }
        if (root.isJsonObject()) {
            JsonObject obj = root.getAsJsonObject();
            for (String key : List.of("Data", "data", "rows", "list", "items", "result")) {
                JsonElement maybeArray = obj.get(key);
                if (maybeArray != null && maybeArray.isJsonArray()) {
                    JsonArray arr = maybeArray.getAsJsonArray();
                    return gson.fromJson(arr, listType);
                }
            }
        }
        // fallback: 返回空列表，避免抛出解析异常
        log.warn("未能从 JSON 解析出数组结构: {}", root);
        return List.of();
    }

    public record SuggestionContext(SuggestionType type, String tableHint, boolean showTableHint, String alias, List<String> scopedTables) {}

    public record TableEntry(String name, String type) {}

    public enum SuggestionType {
        TABLE_OR_VIEW,
        FUNCTION,
        PROCEDURE,
        COLUMN
    }

    public record SuggestionItem(String name, String type, String tableHint, int useCount) {}

    static class RemoteObject {
        String schema_name;
        String object_name;
        String object_type;
    }

    static class RemoteColumn {
        String schema_name;
        String object_name;
        String column_name;
        String sort_no;
    }

    private int parseSortNo(String value, int fallback) {
        if (value == null) return fallback;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
