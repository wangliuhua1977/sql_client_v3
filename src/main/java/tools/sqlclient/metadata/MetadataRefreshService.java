package tools.sqlclient.metadata;

import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.exec.AsyncSqlApiClient;
import tools.sqlclient.exec.OverloadedException;
import tools.sqlclient.exec.ResultExpiredException;
import tools.sqlclient.exec.ResultNotReadyException;
import tools.sqlclient.exec.ResultResponse;
import tools.sqlclient.exec.SubmitResponse;
import tools.sqlclient.util.OperationLog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * 面向 3 万规模元数据的刷新器，遵循 1000 行窗口与异步 SQL 协议。
 */
class MetadataRefreshService {
    private static final int WINDOW = 1000;
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private static final int MAX_OVERLOAD_RETRY = 3;
    private static final int MAX_EXPIRED_RETRY = 1;
    private static final long PER_BATCH_TIMEOUT_MS = Duration.ofSeconds(60).toMillis();
    private static final long POLL_INTERVAL_MIN_MS = 300;
    private static final long POLL_INTERVAL_MAX_MS = 500;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 8000;
    private static final int COLUMN_GROUP_SIZE = 200;

    private final SQLiteManager sqliteManager;
    private final AsyncSqlApiClient apiClient = new AsyncSqlApiClient();
    private final Object dbWriteLock;

    record RefreshStats(boolean success,
                        int totalObjects,
                        int totalColumns,
                        int batches,
                        String message,
                        long durationMillis) {
    }

    record Progress(String phase, int completedBatches, Integer totalBatches, int objects, int columns) {
    }

    MetadataRefreshService(SQLiteManager sqliteManager, Object dbWriteLock) {
        this.sqliteManager = sqliteManager;
        this.dbWriteLock = dbWriteLock;
    }

    RefreshStats refreshAll(String schema,
                            String dbUser,
                            Consumer<Progress> progressConsumer) throws Exception {
        long started = System.currentTimeMillis();
        OperationLog.log("开始 3 万规模元数据刷新，schema=" + schema);
        int batches = 0;
        int objects = 0;
        int columns = 0;
        synchronized (dbWriteLock) {
            try (Connection conn = sqliteManager.getConnection()) {
                prepareStaging(conn);
                clearStaging(conn);
                batches += fetchObjects(schema, dbUser, conn, progressConsumer);
                objects = countTable(conn, "objects_staging");
                batches += fetchColumns(schema, dbUser, conn, objects, progressConsumer);
                columns = countTable(conn, "columns_staging");
                swapFromStaging(conn);
            }
        }
        long cost = System.currentTimeMillis() - started;
        String msg = String.format("刷新完成: objects=%d columns=%d batches=%d 用时 %d ms", objects, columns, batches, cost);
        OperationLog.log(msg);
        return new RefreshStats(true, objects, columns, batches, msg, cost);
    }

    private void prepareStaging(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS objects_staging (" +
                    "schema_name TEXT, object_name TEXT, object_type TEXT, " +
                    "use_count INTEGER DEFAULT 0, last_used_at INTEGER, " +
                    "PRIMARY KEY(schema_name, object_name))");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS columns_staging (" +
                    "schema_name TEXT, object_name TEXT, column_name TEXT, sort_no INTEGER, " +
                    "use_count INTEGER DEFAULT 0, last_used_at INTEGER, " +
                    "PRIMARY KEY(schema_name, object_name, column_name))");
        }
    }

    private void clearStaging(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM objects_staging");
            st.executeUpdate("DELETE FROM columns_staging");
        }
    }

    private int fetchObjects(String schema,
                             String dbUser,
                             Connection conn,
                             Consumer<Progress> progressConsumer) throws Exception {
        int batches = 0;
        batches += fetchByKeyset(schema, dbUser, "table", "information_schema.tables", "table_name",
                "table_schema='%s' AND table_type='BASE TABLE'".formatted(schema), conn, progressConsumer, true);
        batches += fetchByKeyset(schema, dbUser, "view", "information_schema.views", "table_name",
                "table_schema='%s'".formatted(schema), conn, progressConsumer, true);
        batches += fetchByKeyset(schema, dbUser, "function", "pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace",
                "p.proname", "n.nspname='%s' AND (p.prokind='f' OR p.prokind IS NULL AND p.proisagg = FALSE)".formatted(schema),
                conn, progressConsumer, false);
        batches += fetchByKeyset(schema, dbUser, "procedure", "pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace",
                "p.proname", "n.nspname='%s' AND p.prokind='p'".formatted(schema),
                conn, progressConsumer, false);
        return batches;
    }

    private int fetchByKeyset(String schema,
                              String dbUser,
                              String type,
                              String table,
                              String column,
                              String extraCondition,
                              Connection conn,
                              Consumer<Progress> progressConsumer,
                              boolean allowPrefixFallback) throws Exception {
        String lastName = "";
        int batches = 0;
        boolean stalled = false;
        do {
            String where = baseWhere(schema, column, extraCondition, null);
            String sql = buildKeysetSql(table, column, where, lastName);
            ResultResponse resp = executeOne(sql, dbUser, "meta:" + type, lastName);
            batches++;
            List<java.util.Map<String, String>> rows = Optional.ofNullable(resp.getRowMaps()).orElse(List.of());
            if (rows.isEmpty()) {
                break;
            }
            String lastInBatch = lastName;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR IGNORE INTO objects_staging(schema_name, object_name, object_type) VALUES(?,?,?)")) {
                for (java.util.Map<String, String> row : rows) {
                    String name = row.getOrDefault("object_name", null);
                    if (name != null && !name.isBlank()) {
                        ps.setString(1, schema);
                        ps.setString(2, name.trim());
                        ps.setString(3, type);
                        ps.addBatch();
                        lastInBatch = name.trim();
                    }
                }
                ps.executeBatch();
            }
            emitProgress(progressConsumer, "objects:" + type, batches, null, countTable(conn, "objects_staging"), countTable(conn, "columns_staging"));
            if (rows.size() < WINDOW) {
                break;
            }
            if (Objects.equals(lastInBatch, lastName)) {
                stalled = true;
                break;
            }
            lastName = lastInBatch;
        } while (true);

        if (allowPrefixFallback && stalled) {
            batches += fetchByPrefixBuckets(schema, dbUser, type, table, column, extraCondition, conn, progressConsumer);
        }
        return batches;
    }

    private int fetchByPrefixBuckets(String schema,
                                     String dbUser,
                                     String type,
                                     String table,
                                     String column,
                                     String extraCondition,
                                     Connection conn,
                                     Consumer<Progress> progressConsumer) throws Exception {
        List<PrefixBucket> buckets = PrefixBucket.defaults();
        int batches = 0;
        for (PrefixBucket bucket : buckets) {
            String lastName = "";
            while (true) {
                String where = baseWhere(schema, column, extraCondition, bucket);
                String sql = buildKeysetSql(table, column, where, lastName);
                ResultResponse resp = executeOne(sql, dbUser, "meta:" + type + ":" + bucket.label(), lastName);
                batches++;
                List<java.util.Map<String, String>> rows = Optional.ofNullable(resp.getRowMaps()).orElse(List.of());
                if (rows.isEmpty()) {
                    break;
                }
                String lastInBatch = lastName;
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT OR IGNORE INTO objects_staging(schema_name, object_name, object_type) VALUES(?,?,?)")) {
                    for (java.util.Map<String, String> row : rows) {
                        String name = row.getOrDefault("object_name", null);
                        if (name != null && !name.isBlank()) {
                            ps.setString(1, schema);
                            ps.setString(2, name.trim());
                            ps.setString(3, type);
                            ps.addBatch();
                            lastInBatch = name.trim();
                        }
                    }
                    ps.executeBatch();
                }
                emitProgress(progressConsumer, "objects:" + type, batches, null, countTable(conn, "objects_staging"), countTable(conn, "columns_staging"));
                if (rows.size() < WINDOW) {
                    break;
                }
                if (Objects.equals(lastInBatch, lastName)) {
                    break;
                }
                lastName = lastInBatch;
            }
        }
        return batches;
    }

    private int fetchColumns(String schema,
                             String dbUser,
                             Connection conn,
                             int totalObjects,
                             Consumer<Progress> progressConsumer) throws Exception {
        List<String> tables = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT object_name FROM objects_staging WHERE object_type IN ('table','view') ORDER BY object_name");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        int batches = 0;
        List<List<String>> groups = groupTables(tables, COLUMN_GROUP_SIZE);
        for (List<String> group : groups) {
            for (String table : group) {
                String sql = """
                        SELECT table_schema AS schema_name, table_name AS object_name, column_name, data_type, ordinal_position, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_schema='%s' AND table_name = '%s'
                        ORDER BY ordinal_position
                        """.formatted(schema, table.replace("'", "''"));
                ResultResponse resp = executeOne(sql, dbUser, "meta:columns", table);
                batches++;
                List<java.util.Map<String, String>> rows = Optional.ofNullable(resp.getRowMaps()).orElse(List.of());
                try (PreparedStatement psIns = conn.prepareStatement(
                        "INSERT INTO columns_staging(schema_name, object_name, column_name, sort_no) VALUES(?,?,?,?)")) {
                    int idx = 0;
                    for (java.util.Map<String, String> row : rows) {
                        String colName = row.getOrDefault("column_name", null);
                        if (colName == null) {
                            continue;
                        }
                        psIns.setString(1, schema);
                        psIns.setString(2, table);
                        psIns.setString(3, colName);
                        int sortNo = parseInt(row.get("ordinal_position"), ++idx);
                        psIns.setInt(4, sortNo);
                        psIns.addBatch();
                    }
                    psIns.executeBatch();
                }
                emitProgress(progressConsumer, "columns", batches, totalObjects, countTable(conn, "objects_staging"), countTable(conn, "columns_staging"));
            }
        }
        return batches;
    }

    private ResultResponse executeOne(String sql, String dbUser, String label, String lastName) throws Exception {
        int overloadAttempts = 0;
        int expiredAttempts = 0;
        long backoff = INITIAL_BACKOFF_MS;
        while (true) {
            try {
                SubmitResponse submit = apiClient.submit(sql, dbUser, label, WINDOW, DEFAULT_PAGE_SIZE);
                String jobId = submit.getJobId();
                long deadline = System.currentTimeMillis() + PER_BATCH_TIMEOUT_MS;
                ResultResponse ready = waitUntilReady(jobId, label, lastName, deadline);
                ready.setJobId(jobId);
                logBatch(label, jobId, lastName, ready);
                return ready;
            } catch (OverloadedException oe) {
                overloadAttempts++;
                if (overloadAttempts > MAX_OVERLOAD_RETRY) {
                    throw oe;
                }
                sleep(backoff);
                backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
            } catch (ResultExpiredException re) {
                expiredAttempts++;
                if (expiredAttempts > MAX_EXPIRED_RETRY) {
                    throw re;
                }
                OperationLog.log("批次结果过期，自动重提一次 label=" + label + " lastName=" + lastName + " msg=" + re.getMessage());
            }
        }
    }

    private ResultResponse waitUntilReady(String jobId,
                                          String label,
                                          String lastName,
                                          long deadlineMs) throws Exception {
        ResultResponse latest = null;
        while (true) {
            try {
                latest = apiClient.fetchResult(jobId, 1, DEFAULT_PAGE_SIZE, true);
                boolean ready = Boolean.TRUE.equals(latest.getSuccess()) && (latest.getResultAvailable() == null || latest.getResultAvailable());
                boolean stillRunning = latest.getStatus() != null && Set.of("RUNNING", "QUEUED", "ACCEPTED", "CANCELLING")
                        .contains(latest.getStatus().toUpperCase(Locale.ROOT));
                if (latest.getArchiveError() != null && !latest.getArchiveError().isBlank()) {
                    throw new RuntimeException("结果归档失败: " + latest.getArchiveError());
                }
                if (ready && !stillRunning) {
                    return latest;
                }
                if (!ready && !"RESULT_NOT_READY".equalsIgnoreCase(latest.getCode())) {
                    // fallthrough to sleep
                }
            } catch (ResultNotReadyException ignored) {
                // continue polling
            }
            if (System.currentTimeMillis() > deadlineMs) {
                String diag = "等待结果超时 label=" + label + " jobId=" + jobId + " lastName=" + lastName
                        + (latest != null && latest.getQueueDelayMillis() != null ? " queueDelayMillis=" + latest.getQueueDelayMillis() : "");
                throw new RuntimeException(diag);
            }
            sleep(jitter());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private long jitter() {
        return ThreadLocalRandom.current().nextLong(POLL_INTERVAL_MIN_MS, POLL_INTERVAL_MAX_MS + 1);
    }

    private String baseWhere(String schema, String column, String extra, PrefixBucket bucket) {
        StringBuilder sb = new StringBuilder();
        sb.append(column).append(" > '%s'".formatted(""));
        if (extra != null && !extra.isBlank()) {
            sb.append(" AND ").append(extra);
        }
        if (bucket != null && bucket.condition(column) != null) {
            sb.append(" AND ").append(bucket.condition(column));
        }
        return sb.toString();
    }

    private String buildKeysetSql(String table, String column, String whereClause, String lastName) {
        String safeLast = lastName == null ? "" : lastName.replace("'", "''");
        String where = whereClause.replace(column + " > ''", column + " > '" + safeLast + "'");
        return """
                SELECT %s AS object_name
                FROM %s
                WHERE %s
                ORDER BY %s
                LIMIT %d
                """.formatted(column, table, where, column, WINDOW);
    }

    private int countTable(Connection conn, String table) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(1) FROM " + table);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (Exception ignored) {
        }
        return 0;
    }

    private List<List<String>> groupTables(List<String> tables, int groupSize) {
        List<List<String>> groups = new ArrayList<>();
        if (tables == null || tables.isEmpty()) {
            return groups;
        }
        int size = Math.max(1, groupSize);
        for (int i = 0; i < tables.size(); i += size) {
            groups.add(tables.subList(i, Math.min(tables.size(), i + size)));
        }
        return groups;
    }

    private void swapFromStaging(Connection conn) throws Exception {
        boolean auto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM columns");
            st.executeUpdate("DELETE FROM objects");
            st.executeUpdate("DELETE FROM meta_snapshot");
            st.executeUpdate("DELETE FROM columns_snapshot");
            st.executeUpdate("INSERT INTO objects SELECT * FROM objects_staging");
            st.executeUpdate("INSERT INTO columns SELECT * FROM columns_staging");
            st.executeUpdate("DELETE FROM objects_staging");
            st.executeUpdate("DELETE FROM columns_staging");
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(auto);
        }
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

    private void emitProgress(Consumer<Progress> consumer, String phase, int batches, Integer totalBatches, int objects, int columns) {
        if (consumer != null) {
            consumer.accept(new Progress(phase, batches, totalBatches, objects, columns));
        }
    }

    private void logBatch(String label, String jobId, String lastName, ResultResponse resp) {
        int batchSize = resp.getReturnedRowCount() != null ? resp.getReturnedRowCount() : Optional.ofNullable(resp.getRowMaps()).orElse(List.of()).size();
        OperationLog.log("[meta] label=" + label
                + " jobId=" + jobId
                + " lastName=" + OperationLog.abbreviate(lastName == null ? "" : lastName, 50)
                + " batchSize=" + batchSize
                + (resp.getQueueDelayMillis() != null ? (" queueDelayMillis=" + resp.getQueueDelayMillis()) : "")
                + (resp.getOverloaded() != null ? (" overloaded=" + resp.getOverloaded()) : "")
                + (resp.getExpiresAt() != null ? (" expiresAt=" + resp.getExpiresAt()) : "")
                + (resp.getLastAccessAt() != null ? (" lastAccessAt=" + resp.getLastAccessAt()) : "")
                + (resp.getCode() != null ? (" code=" + resp.getCode()) : "")
                + (resp.getThreadPool() != null ? (" threadPool=" + resp.getThreadPool()) : ""));
    }

    ResultResponse runSinglePage(String sql, String dbUser, String label) throws Exception {
        return executeOne(sql, dbUser, label, "");
    }

    private record PrefixBucket(String label, String fromInclusive, String toExclusive) {
        String condition(String column) {
            if ("other".equals(label)) {
                return "lower(" + column + ") < '0' OR lower(" + column + ") >= '{'";
            }
            return "lower(" + column + ") >= '" + fromInclusive + "' AND lower(" + column + ") < '" + toExclusive + "'";
        }

        static List<PrefixBucket> defaults() {
            List<PrefixBucket> buckets = new ArrayList<>();
            buckets.add(new PrefixBucket("0-9", "0", ":"));
            buckets.add(new PrefixBucket("a-c", "a", "d"));
            buckets.add(new PrefixBucket("d-f", "d", "g"));
            buckets.add(new PrefixBucket("g-i", "g", "j"));
            buckets.add(new PrefixBucket("j-l", "j", "m"));
            buckets.add(new PrefixBucket("m-o", "m", "p"));
            buckets.add(new PrefixBucket("p-r", "p", "s"));
            buckets.add(new PrefixBucket("s-u", "s", "v"));
            buckets.add(new PrefixBucket("v-w", "v", "x"));
            buckets.add(new PrefixBucket("x-z", "x", "{"));
            buckets.add(new PrefixBucket("other", null, null));
            return buckets;
        }
    }
}
