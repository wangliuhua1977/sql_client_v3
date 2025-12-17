package tools.sqlclient.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.util.OperationLog;

import javax.swing.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.security.MessageDigest;
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
    private static final String DEFAULT_SCHEMA = "leshan";
    private static final String METADATA_DB_USER = "leshan";
    private static final int METADATA_BATCH_SIZE = 200;
    private static final int TRACE_ROW_LIMIT = 200;

    private final SQLiteManager sqliteManager;
    private final ExecutorService metadataPool = Executors.newFixedThreadPool(4, r -> new Thread(r, "metadata-pool"));
    private final ExecutorService networkPool = Executors.newFixedThreadPool(6, r -> new Thread(r, "network-pool"));
    private final SqlExecutionService sqlExecutionService = new SqlExecutionService();
    private final Object dbWriteLock = new Object();
    private final Set<String> inflightColumns = java.util.Collections.synchronizedSet(new HashSet<>());

    public record MetadataRefreshResult(boolean success, int totalObjects, int changedObjects, String message) {}
    private record RemoteSnapshot(Map<String, List<String>> objects, Map<String, List<String>> columnsByObject, int totalColumns) {}

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
        long startedAt = System.currentTimeMillis();
        try {
            OperationLog.log("开始刷新对象元数据");
            RemoteSnapshot snapshot = fetchRemoteSnapshot();
            int totalChanges = 0;
            int totalObjects;
            synchronized (dbWriteLock) {
                try (Connection conn = sqliteManager.getConnection()) {
                    conn.setAutoCommit(false);
                    Map<String, String> snapshots = loadSnapshots(conn);
                    totalChanges += syncObjects(conn, "table", snapshot.objects().get("table"), snapshots);
                    totalChanges += syncObjects(conn, "view", snapshot.objects().get("view"), snapshots);
                    totalChanges += syncObjects(conn, "function", snapshot.objects().get("function"), snapshots);
                    totalChanges += syncObjects(conn, "procedure", snapshot.objects().get("procedure"), snapshots);
                    totalChanges += syncColumns(conn, snapshot.columnsByObject(), snapshots);
                    conn.commit();
                }
            }
            totalObjects = countCachedObjects();
            long cost = System.currentTimeMillis() - startedAt;
            OperationLog.log(String.format("对象同步完成：表 %d，视图 %d，函数 %d，过程 %d，字段 %d，变更 %d，用时 %d ms",
                    snapshot.objects().getOrDefault("table", List.of()).size(),
                    snapshot.objects().getOrDefault("view", List.of()).size(),
                    snapshot.objects().getOrDefault("function", List.of()).size(),
                    snapshot.objects().getOrDefault("procedure", List.of()).size(),
                    snapshot.totalColumns(),
                    totalChanges,
                    cost));
            return new MetadataRefreshResult(true, totalObjects, totalChanges, "刷新成功，用时 " + cost + " ms");
        } catch (Exception e) {
            log.error("刷新元数据失败", e);
            OperationLog.log("刷新元数据失败: " + e.getMessage());
            return new MetadataRefreshResult(false, countCachedObjects(), 0, e.getMessage());
        }
    }

    private RemoteSnapshot fetchRemoteSnapshot() throws Exception {
        Map<String, List<String>> objects = new HashMap<>();
        objects.put("table", fetchObjects("table",
                String.format("""
                        SELECT table_name AS object_name
                        FROM information_schema.tables
                        WHERE table_schema='%s' AND table_type='BASE TABLE'
                        ORDER BY table_name
                        LIMIT %d OFFSET %%d
                        """, DEFAULT_SCHEMA, METADATA_BATCH_SIZE)));
        objects.put("view", fetchObjects("view", String.format("""
                SELECT table_name AS object_name
                FROM information_schema.views
                WHERE table_schema='%s'
                ORDER BY table_name
                LIMIT %d OFFSET %%d
                """, DEFAULT_SCHEMA, METADATA_BATCH_SIZE)));
        objects.put("function", fetchObjects("function", String.format("""
                SELECT p.proname AS object_name
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='%s' AND p.prokind='f'
                ORDER BY p.proname
                LIMIT %d OFFSET %%d
                """, DEFAULT_SCHEMA, METADATA_BATCH_SIZE)));
        objects.put("procedure", fetchObjects("procedure", String.format("""
                SELECT p.proname AS object_name
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='%s' AND p.prokind='p'
                ORDER BY p.proname
                LIMIT %d OFFSET %%d
                """, DEFAULT_SCHEMA, METADATA_BATCH_SIZE)));
        Map<String, List<String>> columns = fetchColumns();
        return new RemoteSnapshot(objects, columns, countColumns(columns));
    }

    private List<String> fetchObjects(String type, String pagedSqlTemplate) throws Exception {
        List<String> names = new ArrayList<>();
        int offset = 0;
        while (true) {
            String sql = String.format(pagedSqlTemplate, offset);
            SqlExecResult result = runMetadataSql(sql);
            recordTrace(type + "-" + offset, sql, result);
            List<List<String>> rows = result.getRows();
            if (rows == null || rows.isEmpty()) {
                break;
            }
            Map<String, Integer> idx = indexColumns(result.getColumns());
            int nameIdx = idx.getOrDefault("object_name", -1);
            for (List<String> row : rows) {
                String name = valueAt(row, nameIdx);
                if (name != null && !name.isBlank()) {
                    names.add(name.trim());
                }
            }
            OperationLog.log(String.format("[%s] 已拉取 %d 条，offset=%d", type, names.size(), offset));
            if (rows.size() < METADATA_BATCH_SIZE) {
                break;
            }
            offset += rows.size();
        }
        return names;
    }

    private Map<String, List<String>> fetchColumns() throws Exception {
        Map<String, List<String>> columns = new LinkedHashMap<>();
        int offset = 0;
        int total = 0;
        while (true) {
            String sql = String.format("""
                    SELECT table_schema AS schema_name, table_name AS object_name, column_name, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema='%s'
                    ORDER BY table_name, ordinal_position
                    LIMIT %d OFFSET %d
                    """, DEFAULT_SCHEMA, METADATA_BATCH_SIZE, offset);
            SqlExecResult result = runMetadataSql(sql);
            recordTrace("columns-" + offset, sql, result);
            List<List<String>> rows = result.getRows();
            if (rows == null || rows.isEmpty()) {
                break;
            }
            Map<String, Integer> idx = indexColumns(result.getColumns());
            int objIdx = idx.getOrDefault("object_name", -1);
            int nameIdx = idx.getOrDefault("column_name", -1);
            for (List<String> row : rows) {
                String obj = valueAt(row, objIdx);
                String col = valueAt(row, nameIdx);
                if (obj == null || col == null) continue;
                columns.computeIfAbsent(obj, k -> new ArrayList<>()).add(col);
                total++;
            }
            OperationLog.log(String.format("[columns] 已拉取 %d 列，offset=%d", total, offset));
            if (rows.size() < METADATA_BATCH_SIZE) {
                break;
            }
            offset += rows.size();
        }
        return columns;
    }

    private Map<String, String> loadSnapshots(Connection conn) {
        Map<String, String> snapshot = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT meta_type, meta_hash FROM meta_snapshot")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    snapshot.put(rs.getString("meta_type"), rs.getString("meta_hash"));
                }
            }
        } catch (Exception e) {
            log.warn("读取 meta_snapshot 失败", e);
        }
        return snapshot;
    }

    private void saveSnapshot(Connection conn, String type, String hash) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO meta_snapshot(meta_type, meta_hash, updated_at) VALUES(?,?,?) " +
                "ON CONFLICT(meta_type) DO UPDATE SET meta_hash=excluded.meta_hash, updated_at=excluded.updated_at")) {
            ps.setString(1, type);
            ps.setString(2, hash);
            ps.setLong(3, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    private String digest(String payload) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = md.digest(Optional.ofNullable(payload).orElse("").getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    private SqlExecResult runMetadataSql(String sql) throws Exception {
        OperationLog.log("执行元数据 SQL: " + OperationLog.abbreviate(sql.replaceAll("\\s+", " ").trim(), 200));
        return sqlExecutionService.executeSyncAllPagesWithDbUser(sql, METADATA_DB_USER);
    }

    private int syncObjects(Connection conn, String type, List<String> names, Map<String, String> snapshots) throws Exception {
        if (names == null) names = List.of();
        String hash = digest(String.join("\n", names));
        if (hash.equals(snapshots.get(type))) {
            OperationLog.log(type + " 未变化，跳过写入");
            return 0;
        }
        int changes = upsertObjects(conn, type, names);
        saveSnapshot(conn, type, hash);
        return changes;
    }

    private int syncColumns(Connection conn, Map<String, List<String>> remote, Map<String, String> snapshots) throws Exception {
        if (remote == null) remote = Map.of();
        String hash = digestColumns(remote);
        if (hash.equals(snapshots.get("columns"))) {
            OperationLog.log("columns 未变化，跳过写入");
            return 0;
        }
        int changes = updateColumnsDiff(conn, remote);
        saveSnapshot(conn, "columns", hash);
        return changes;
    }

    private String digestColumns(Map<String, List<String>> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        List<String> lines = new ArrayList<>();
        columns.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> lines.add(e.getKey() + "\t" + String.join(",", e.getValue())));
        return digest(String.join("\n", lines));
    }

    private int countColumns(Map<String, List<String>> columns) {
        if (columns == null || columns.isEmpty()) {
            return 0;
        }
        return columns.values().stream().mapToInt(List::size).sum();
    }

    private void recordTrace(String tag, String sql, SqlExecResult result) {
        try {
            String safeTag = tag.replaceAll("[^A-Za-z0-9_-]", "_");
            Path dir = Paths.get(System.getProperty("user.home"), ".Sql_client_v3", "logs");
            Files.createDirectories(dir);
            Path file = dir.resolve(String.format("metadata-%s-%d.log", safeTag, System.currentTimeMillis()));
            StringBuilder sb = new StringBuilder();
            sb.append("sql: ").append(sql).append(System.lineSeparator());
            if (result != null) {
                sb.append("jobId: ").append(Optional.ofNullable(result.getJobId()).orElse("<unknown>"))
                        .append(" status: ").append(Optional.ofNullable(result.getStatus()).orElse(""))
                        .append(" page: ").append(Optional.ofNullable(result.getPage()).orElse(-1))
                        .append(" pageSize: ").append(Optional.ofNullable(result.getPageSize()).orElse(-1))
                        .append(System.lineSeparator());
                List<List<String>> rows = result.getRows();
                if (rows != null) {
                    sb.append("rows (up to ").append(TRACE_ROW_LIMIT).append("):").append(System.lineSeparator());
                    int limit = Math.min(rows.size(), TRACE_ROW_LIMIT);
                    for (int i = 0; i < limit; i++) {
                        sb.append(String.join("\t", rows.get(i))).append(System.lineSeparator());
                    }
                    if (rows.size() > TRACE_ROW_LIMIT) {
                        sb.append("<trimmed ").append(rows.size() - TRACE_ROW_LIMIT).append(" rows>").append(System.lineSeparator());
                    }
                }
            }
            Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
            if (safeTag.endsWith("-0")) {
                OperationLog.log("元数据请求/响应已写入: " + file.toAbsolutePath());
            } else {
                log.debug("元数据请求/响应已写入: {}", file.toAbsolutePath());
            }
        } catch (Exception e) {
            log.debug("写入元数据调试日志失败", e);
        }
    }

    private int upsertObjects(Connection conn, String type, List<String> names) throws Exception {
        Set<String> remote = new HashSet<>(names);
        Set<String> local = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT object_name FROM objects WHERE object_type=?")) {
            ps.setString(1, type);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    local.add(rs.getString(1));
                }
            }
        }
        int changes = 0;
        try (PreparedStatement ins = conn.prepareStatement("INSERT OR IGNORE INTO objects(schema_name, object_name, object_type) VALUES(?,?,?)");
             PreparedStatement del = conn.prepareStatement("DELETE FROM objects WHERE object_name=? AND object_type=?")) {
            for (String name : remote) {
                ins.setString(1, DEFAULT_SCHEMA);
                ins.setString(2, name);
                ins.setString(3, type);
                changes += ins.executeUpdate();
            }
            for (String old : local) {
                if (!remote.contains(old)) {
                    del.setString(1, old);
                    del.setString(2, type);
                    changes += del.executeUpdate();
                }
            }
        }
        return changes;
    }

    private int updateColumnsDiff(Connection conn, Map<String, List<String>> columnsPayload) throws Exception {
        int changes = 0;
        Map<String, List<String>> local = new HashMap<>();
        Map<String, String> objectTypes = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT object_name, object_type FROM objects")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    objectTypes.put(rs.getString("object_name"), rs.getString("object_type"));
                }
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("SELECT object_name, column_name, sort_no FROM columns ORDER BY sort_no")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String obj = rs.getString("object_name");
                    String col = rs.getString("column_name");
                    local.computeIfAbsent(obj, k -> new ArrayList<>()).add(col);
                }
            }
        }

        try (PreparedStatement delObj = conn.prepareStatement("DELETE FROM columns WHERE object_name=?");
             PreparedStatement ins = conn.prepareStatement("INSERT INTO columns(schema_name, object_name, column_name, sort_no) VALUES(?,?,?,?)");
             PreparedStatement ensureObj = conn.prepareStatement("INSERT OR IGNORE INTO objects(schema_name, object_name, object_type) VALUES(?,?,?)")) {
            for (Map.Entry<String, List<String>> entry : columnsPayload.entrySet()) {
                String obj = entry.getKey();
                List<String> remoteCols = entry.getValue();
                List<String> localCols = local.getOrDefault(obj, List.of());
                if (!remoteCols.equals(localCols)) {
                    delObj.setString(1, obj);
                    delObj.executeUpdate();
                    for (int i = 0; i < remoteCols.size(); i++) {
                        ins.setString(1, DEFAULT_SCHEMA);
                        ins.setString(2, obj);
                        ins.setString(3, remoteCols.get(i));
                        ins.setInt(4, i + 1);
                        ins.addBatch();
                    }
                    ins.executeBatch();
                    ensureObj.setString(1, DEFAULT_SCHEMA);
                    ensureObj.setString(2, obj);
                    ensureObj.setString(3, objectTypes.getOrDefault(obj, "table"));
                    ensureObj.executeUpdate();
                    changes++;
                }
            }

            for (String existing : local.keySet()) {
                if (!columnsPayload.containsKey(existing)) {
                    delObj.setString(1, existing);
                    changes += delObj.executeUpdate();
                }
            }
        }
        return changes;
    }

    private void clearLocalMetadata() {
        synchronized (dbWriteLock) {
            try (Connection conn = sqliteManager.getConnection(); Statement st = conn.createStatement()) {
                boolean auto = conn.getAutoCommit();
                if (auto) conn.setAutoCommit(false);
                st.executeUpdate("DELETE FROM columns");
                st.executeUpdate("DELETE FROM objects");
                st.executeUpdate("DELETE FROM meta_snapshot");
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

    private List<RemoteColumn> fetchColumnsBySql(String objectName) throws Exception {
        String safeName = sanitizeObjectName(objectName);
        if (safeName == null) {
            return List.of();
        }
        String sql = String.format("""
                SELECT table_schema AS schema_name, table_name AS object_name, column_name, data_type, ordinal_position, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema='%s' AND table_name = '%s'
                ORDER BY ordinal_position
                """, DEFAULT_SCHEMA, safeName);
        SqlExecResult result = runMetadataSql(sql);
        return toColumns(result);
    }

    private String sanitizeObjectName(String objectName) {
        if (!isLikelyTableName(objectName)) {
            return null;
        }
        return objectName.replace("'", "''");
    }

    private List<RemoteColumn> toColumns(SqlExecResult result) {
        if (result == null || result.getRows() == null || result.getColumns() == null) {
            return List.of();
        }
        Map<String, Integer> idx = indexColumns(result.getColumns());
        int schemaIdx = idx.getOrDefault("schema_name", -1);
        int objIdx = idx.getOrDefault("object_name", -1);
        int nameIdx = idx.getOrDefault("column_name", -1);
        int typeIdx = idx.getOrDefault("data_type", -1);
        int ordinalIdx = idx.getOrDefault("ordinal_position", -1);
        int nullableIdx = idx.getOrDefault("is_nullable", -1);
        int defaultIdx = idx.getOrDefault("column_default", -1);

        List<RemoteColumn> list = new ArrayList<>();
        for (List<String> row : result.getRows()) {
            String name = valueAt(row, nameIdx);
            if (name == null) continue;
            String schema = valueAt(row, schemaIdx);
            String object = valueAt(row, objIdx);
            int ordinal = parseInt(valueAt(row, ordinalIdx), list.size() + 1);
            String dataType = valueAt(row, typeIdx);
            String nullable = valueAt(row, nullableIdx);
            String columnDefault = valueAt(row, defaultIdx);
            list.add(new RemoteColumn(schema != null ? schema : DEFAULT_SCHEMA, objectNameOrDefault(object), name, dataType, ordinal, nullable, columnDefault));
        }
        return list;
    }

    private Map<String, Integer> indexColumns(List<String> columns) {
        Map<String, Integer> idx = new HashMap<>();
        if (columns == null) return idx;
        for (int i = 0; i < columns.size(); i++) {
            idx.put(columns.get(i).toLowerCase(Locale.ROOT), i);
        }
        return idx;
    }

    private String valueAt(List<String> row, int idx) {
        if (row == null || idx < 0 || idx >= row.size()) {
            return null;
        }
        return row.get(idx);
    }

    private String objectNameOrDefault(String name) {
        return name != null ? name : "";
    }

    private int parseInt(String value, int fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private void updateColumns(String objectName, boolean forceRefresh) {
        boolean knownTableOrView = isTableOrView(objectName);
        int attempts = 0;
        while (attempts < 3) {
            try {
                attempts++;
                List<RemoteColumn> columns = fetchColumnsBySql(objectName);
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
                                    int remoteSort = col.ordinal_position;
                                    if (localSort == null || localSort != remoteSort) {
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
                                        ins.setInt(4, col.ordinal_position > 0 ? col.ordinal_position : i++);
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

    public record SuggestionContext(SuggestionType type, String tableHint, boolean showTableHint, String alias, List<String> scopedTables) {}

    public record TableEntry(String name, String type) {}

    public enum SuggestionType {
        TABLE_OR_VIEW,
        FUNCTION,
        PROCEDURE,
        COLUMN
    }

    public record SuggestionItem(String name, String type, String tableHint, int useCount) {}

    static class RemoteColumn {
        String schema_name;
        String object_name;
        String column_name;
        String data_type;
        int ordinal_position;
        String is_nullable;
        String column_default;

        RemoteColumn(String schema_name, String object_name, String column_name, String data_type, int ordinal_position, String is_nullable, String column_default) {
            this.schema_name = schema_name;
            this.object_name = object_name;
            this.column_name = column_name;
            this.data_type = data_type;
            this.ordinal_position = ordinal_position;
            this.is_nullable = is_nullable;
            this.column_default = column_default;
        }
    }
}
