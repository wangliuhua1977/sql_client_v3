package tools.sqlclient.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.exec.OverloadedException;
import tools.sqlclient.exec.ResultExpiredException;
import tools.sqlclient.exec.ResultNotReadyException;
import tools.sqlclient.exec.ResultResponse;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.util.Config;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 管理元数据：差分更新、本地缓存、模糊匹配。
 */
public class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    private static final String DEFAULT_SCHEMA = "leshan";
    private static final String METADATA_DB_USER = "leshan";
    private static final int METADATA_BATCH_SIZE = 1000;
    private static final int TRACE_ROW_LIMIT = 200;
    private static final Pattern DDL_PATTERN = Pattern.compile(
            "^(?i)(create|drop|alter)\\s+(or\\s+replace\\s+)?(table|view|function|procedure)\\s+(if\\s+not\\s+exists\\s+|if\\s+exists\\s+)?([A-Za-z0-9_\\.\\\"]+)");

    private final SQLiteManager sqliteManager;
    private final ExecutorService metadataPool = Executors.newFixedThreadPool(resolveMetadataConcurrency(), r -> new Thread(r, "metadata-pool"));
    private final ExecutorService networkPool = Executors.newFixedThreadPool(resolveNetworkConcurrency(), r -> new Thread(r, "network-pool"));
    private final MetadataRefreshService refreshService;
    private final Object dbWriteLock = new Object();
    private final Set<String> inflightColumns = java.util.Collections.synchronizedSet(new HashSet<>());

    public record MetadataRefreshResult(boolean success,
                                        int totalObjects,
                                        int changedObjects,
                                        int totalColumns,
                                        int batches,
                                        long durationMillis,
                                        String message) {}

    public MetadataService(Path dbPath) {
        this.sqliteManager = new SQLiteManager(dbPath);
        this.sqliteManager.initSchema();
        this.refreshService = new MetadataRefreshService(sqliteManager, dbWriteLock);
    }

    private static int resolveMetadataConcurrency() {
        int configured = readInt("METADATA_REFRESH_CONCURRENCY", 1);
        if (configured < 1) {
            configured = 1;
        }
        if (configured > 2) {
            configured = 2;
        }
        return configured;
    }

    private static int resolveNetworkConcurrency() {
        int configured = readInt("METADATA_NETWORK_CONCURRENCY", resolveMetadataConcurrency());
        if (configured < 1) {
            configured = 1;
        }
        if (configured > 2) {
            configured = 2;
        }
        return configured;
    }

    private static int readInt(String key, int defaultValue) {
        String sys = System.getProperty(key);
        if (sys != null && !sys.isBlank()) {
            try {
                return Integer.parseInt(sys.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        String raw = Config.getRawProperty(key);
        if (raw != null && !raw.isBlank()) {
            try {
                return Integer.parseInt(raw.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
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
                        return new MetadataRefreshResult(false, countCachedObjects(), 0, countCachedColumns(), 0, 0L, ex.getMessage());
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
                return new MetadataRefreshResult(false, countCachedObjects(), 0, countCachedColumns(), 0, 0L, ex.getMessage());
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
        int beforeObjects = countCachedObjects();
        try {
            MetadataRefreshService.RefreshStats stats = refreshService.refreshAll(DEFAULT_SCHEMA, METADATA_DB_USER, this::handleRefreshProgress);
            int totalObjects = stats.totalObjects();
            int changed = Math.max(0, totalObjects - beforeObjects);
            return new MetadataRefreshResult(true, totalObjects, changed, stats.totalColumns(), stats.batches(), stats.durationMillis(), stats.message());
        } catch (Exception e) {
            log.error("刷新元数据失败", e);
            OperationLog.log("刷新元数据失败: " + e.getMessage());
            long cost = System.currentTimeMillis() - startedAt;
            return new MetadataRefreshResult(false, countCachedObjects(), 0, countCachedColumns(), 0, cost, e.getMessage());
        }
    }

    public List<String> fetchNamesByKeyset(String sqlTemplate, int batchSize) throws Exception {
        List<String> names = new ArrayList<>();
        String lastName = "";
        while (true) {
            String sql = String.format(sqlTemplate, escapeLiteral(lastName), batchSize);
            SqlExecResult result = runMetadataSql(sql);
            recordTrace("keyset-" + abbreviateTag(sqlTemplate, lastName), sql, result);
            List<List<String>> rows = result.getRows();
            if (rows == null || rows.isEmpty()) {
                break;
            }
            Map<String, Integer> idx = indexColumns(result.getColumns());
            int nameIdx = idx.getOrDefault("object_name", -1);
            String lastInBatch = lastName;
            for (List<String> row : rows) {
                String name = valueAt(row, nameIdx);
                if (name != null && !name.isBlank()) {
                    names.add(name.trim());
                    lastInBatch = name.trim();
                }
            }
            OperationLog.log(String.format("[keyset] 已拉取 %d 条，lastName=%s", names.size(), lastInBatch));
            if (rows.size() < batchSize) {
                break;
            }
            if (lastInBatch.equals(lastName)) {
                break;
            }
            lastName = lastInBatch;
        }
        return names;
    }

    private SqlExecResult runMetadataSql(String sql) throws Exception {
        OperationLog.log("执行元数据 SQL: " + OperationLog.abbreviate(sql.replaceAll("\\s+", " ").trim(), 200));
        int expiredAttempts = 0;
        int overloadAttempts = 0;
        long backoff = 500;
        while (true) {
            try {
                ResultResponse resp = refreshService.runSinglePage(sql, METADATA_DB_USER, "meta:adhoc");
                return toSqlExecResult(sql, resp);
            } catch (ResultExpiredException rex) {
                expiredAttempts++;
                if (expiredAttempts > 1) {
                    throw rex;
                }
                OperationLog.log("元数据结果过期，自动重试一次: " + rex.getMessage());
            } catch (ResultNotReadyException rnre) {
                try { Thread.sleep(300); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            } catch (OverloadedException oe) {
                overloadAttempts++;
                if (overloadAttempts > 3) {
                    throw oe;
                }
                try { Thread.sleep(backoff); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                backoff = Math.min(8000, backoff * 2);
            }
        }
    }

    private SqlExecResult toSqlExecResult(String sql, ResultResponse resp) {
        List<java.util.Map<String, String>> rowMaps = resp.getRowMaps() != null ? resp.getRowMaps() : List.of();
        List<String> columns = resp.getColumns() != null ? resp.getColumns() : List.of();
        List<List<String>> rows = new ArrayList<>();
        for (java.util.Map<String, String> map : rowMaps) {
            List<String> row = new ArrayList<>();
            for (String col : columns) {
                row.add(map.get(col));
            }
            rows.add(row);
        }
        int rowCount = resp.getReturnedRowCount() != null ? resp.getReturnedRowCount() : rowMaps.size();
        return new SqlExecResult(sql, columns, null, rows, rowMaps, rowCount, Boolean.TRUE.equals(resp.getSuccess()),
                resp.getMessage(), resp.getJobId(), resp.getStatus(), null, null, null, null,
                resp.getReturnedRowCount(), resp.getActualRowCount(), resp.getMaxVisibleRows(), resp.getMaxTotalRows(),
                resp.getHasResultSet(), resp.getPage(), resp.getPageSize(), resp.getHasNext(), resp.getTruncated(),
                resp.getMessage(), null, resp.getQueueDelayMillis(), resp.getOverloaded(), resp.getThreadPool());
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
             PreparedStatement del = conn.prepareStatement("DELETE FROM objects WHERE object_name=? AND object_type=?");
             PreparedStatement dropColumns = conn.prepareStatement("DELETE FROM columns WHERE object_name=?")) {
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
                    if ("table".equals(type) || "view".equals(type)) {
                        dropColumns.setString(1, old);
                        changes += dropColumns.executeUpdate();
                    }
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
                st.executeUpdate("DELETE FROM columns_snapshot");
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

    private int countCachedColumns() {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(1) FROM columns")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Exception e) {
            log.warn("统计本地字段数量失败", e);
        }
        return 0;
    }

    public void handleSqlSucceeded(String sql) {
        DdlInfo info = parseDdl(sql);
        if (info == null) {
            return;
        }
        switch (info.action()) {
            case "create" -> refreshObjectByName(info);
            case "drop" -> removeLocalObject(info);
            case "alter" -> {
                if (isTableOrViewType(info.type())) {
                    ensureColumnsCachedAsync(info.objectName(), true);
                }
                refreshObjectByName(info);
            }
            default -> {
            }
        }
    }

    private void refreshObjectByName(DdlInfo info) {
        if (info == null) {
            return;
        }
        if (!DEFAULT_SCHEMA.equalsIgnoreCase(info.schema())) {
            OperationLog.log("检测到 schema=" + info.schema() + "，当前仅刷新默认 schema " + DEFAULT_SCHEMA);
        }
        try {
            List<String> names = fetchNamesByExactType(info.type(), info.objectName());
            synchronized (dbWriteLock) {
                try (Connection conn = sqliteManager.getConnection()) {
                    boolean auto = conn.getAutoCommit();
                    if (auto) {
                        conn.setAutoCommit(false);
                    }
                    if (!names.isEmpty()) {
                        upsertSingleObject(conn, info.type(), names.get(0));
                    } else {
                        deleteObject(conn, info.type(), info.objectName());
                    }
                    conn.commit();
                    if (auto) {
                        conn.setAutoCommit(true);
                    }
                }
            }
            OperationLog.log("DDL 后已刷新 " + info.type() + " " + info.objectName()
                    + (names.isEmpty() ? "（远端不存在，已移除本地）" : ""));
        } catch (Exception e) {
            log.warn("DDL 后刷新对象失败: {}", info.objectName(), e);
            OperationLog.log("DDL 后刷新对象失败: " + info.objectName() + " | " + e.getMessage());
        }
    }

    private void removeLocalObject(DdlInfo info) {
        synchronized (dbWriteLock) {
            try (Connection conn = sqliteManager.getConnection()) {
                boolean auto = conn.getAutoCommit();
                if (auto) {
                    conn.setAutoCommit(false);
                }
                deleteObject(conn, info.type(), info.objectName());
                conn.commit();
                if (auto) {
                    conn.setAutoCommit(true);
                }
            } catch (Exception e) {
                log.warn("删除本地对象失败: {}", info.objectName(), e);
            }
        }
        OperationLog.log("已删除本地缓存: " + info.type() + " " + info.objectName());
    }

    private void handleRefreshProgress(MetadataRefreshService.Progress progress) {
        if (progress == null) {
            return;
        }
        String text = String.format("刷新进度[%s] 批次=%d%s 对象=%d 列=%d",
                progress.phase(),
                progress.completedBatches(),
                progress.totalBatches() != null ? ("/" + progress.totalBatches()) : "",
                progress.objects(),
                progress.columns());
        OperationLog.log(text);
    }

    private List<String> fetchNamesByExactType(String type, String objectName) throws Exception {
        String safeName = sanitizeObjectName(objectName);
        if (safeName == null) {
            return List.of();
        }
        String sql;
        switch (type) {
            case "table" -> sql = String.format("""
                    SELECT table_name AS object_name
                    FROM information_schema.tables
                    WHERE table_schema='%s' AND table_type='BASE TABLE' AND table_name = '%s'
                    ORDER BY table_name
                    LIMIT %d
                    """, DEFAULT_SCHEMA, safeName, METADATA_BATCH_SIZE);
            case "view" -> sql = String.format("""
                    SELECT table_name AS object_name
                    FROM information_schema.views
                    WHERE table_schema='%s' AND table_name = '%s'
                    ORDER BY table_name
                    LIMIT %d
                    """, DEFAULT_SCHEMA, safeName, METADATA_BATCH_SIZE);
            case "function" -> sql = String.format("""
                    SELECT p.proname AS object_name
                    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                    WHERE n.nspname='%s' AND p.prokind='f' AND p.proname = '%s'
                    ORDER BY p.proname
                    LIMIT %d
                    """, DEFAULT_SCHEMA, safeName, METADATA_BATCH_SIZE);
            case "procedure" -> sql = String.format("""
                    SELECT p.proname AS object_name
                    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                    WHERE n.nspname='%s' AND p.prokind='p' AND p.proname = '%s'
                    ORDER BY p.proname
                    LIMIT %d
                    """, DEFAULT_SCHEMA, safeName, METADATA_BATCH_SIZE);
            default -> {
                return List.of();
            }
        }
        SqlExecResult result = runMetadataSql(sql);
        recordTrace("single-" + type + "-" + abbreviateTag(objectName, objectName), sql, result);
        List<String> names = new ArrayList<>();
        Map<String, Integer> idx = indexColumns(result.getColumns());
        int nameIdx = idx.getOrDefault("object_name", -1);
        if (result.getRows() != null) {
            for (List<String> row : result.getRows()) {
                String name = valueAt(row, nameIdx);
                if (name != null && !name.isBlank()) {
                    names.add(name.trim());
                }
            }
        }
        return names;
    }

    private void upsertSingleObject(Connection conn, String type, String name) throws Exception {
        try (PreparedStatement ins = conn.prepareStatement("INSERT OR IGNORE INTO objects(schema_name, object_name, object_type) VALUES(?,?,?)");
             PreparedStatement upd = conn.prepareStatement("UPDATE objects SET object_type=? WHERE object_name=?")) {
            ins.setString(1, DEFAULT_SCHEMA);
            ins.setString(2, name);
            ins.setString(3, type);
            ins.executeUpdate();
            upd.setString(1, type);
            upd.setString(2, name);
            upd.executeUpdate();
        }
    }

    private void deleteObject(Connection conn, String type, String name) throws Exception {
        try (PreparedStatement del = conn.prepareStatement("DELETE FROM objects WHERE object_name=? AND object_type=?")) {
            del.setString(1, name);
            del.setString(2, type);
            del.executeUpdate();
        }
        if (isTableOrViewType(type)) {
            dropColumnsCache(conn, name);
        }
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

    private String escapeLiteral(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private String abbreviateTag(String base, String lastName) {
        String suffix = lastName == null || lastName.isBlank() ? "start" : lastName;
        return OperationLog.abbreviate(base, 20) + "-" + OperationLog.abbreviate(suffix, 20);
    }

    private void dropColumnsCache(Connection conn, String objectName) throws Exception {
        try (PreparedStatement delCols = conn.prepareStatement("DELETE FROM columns WHERE object_name=?");
             PreparedStatement delSnap = conn.prepareStatement("DELETE FROM columns_snapshot WHERE object_name=?")) {
            delCols.setString(1, objectName);
            delCols.executeUpdate();
            delSnap.setString(1, objectName);
            delSnap.executeUpdate();
        }
    }

    private DdlInfo parseDdl(String sql) {
        if (sql == null) {
            return null;
        }
        String normalized = sql.replaceAll("[\\r\\n]+", " ").trim();
        Matcher matcher = DDL_PATTERN.matcher(normalized);
        if (!matcher.find()) {
            return null;
        }
        String action = matcher.group(1).toLowerCase(Locale.ROOT);
        String type = matcher.group(3).toLowerCase(Locale.ROOT);
        String rawName = matcher.group(5);
        if (rawName == null || rawName.isBlank()) {
            return null;
        }
        String cleaned = rawName.replace("\"", "").replaceAll(";+", "").trim();
        String schema = DEFAULT_SCHEMA;
        if (cleaned.contains(".")) {
            String[] parts = cleaned.split("\\.", 2);
            if (parts.length == 2) {
                schema = parts[0];
                cleaned = parts[1];
            }
        }
        return new DdlInfo(action, type, schema, cleaned);
    }

    private boolean isTableOrViewType(String type) {
        return "table".equalsIgnoreCase(type) || "view".equalsIgnoreCase(type);
    }

    private String computeColumnsHash(List<RemoteColumn> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        List<String> names = columns.stream()
                .sorted(Comparator.comparingInt(c -> c.ordinal_position))
                .map(c -> c.column_name)
                .collect(Collectors.toList());
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(String.join(",", names).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
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
                        try (PreparedStatement snapshotPs = conn.prepareStatement("INSERT INTO columns_snapshot(object_name, cols_hash, updated_at) VALUES(?,?,?) ON CONFLICT(object_name) DO UPDATE SET cols_hash=excluded.cols_hash, updated_at=excluded.updated_at")) {
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
                                String colsHash = computeColumnsHash(columns);
                                snapshotPs.setString(1, objectName);
                                snapshotPs.setString(2, colsHash);
                                snapshotPs.setLong(3, System.currentTimeMillis());
                                snapshotPs.executeUpdate();
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

    private record DdlInfo(String action, String type, String schema, String objectName) {}

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
