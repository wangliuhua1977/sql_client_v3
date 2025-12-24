package tools.sqlclient.metadata;

import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.exec.AsyncJobStatus;
import tools.sqlclient.exec.OverloadedException;
import tools.sqlclient.exec.ResultExpiredException;
import tools.sqlclient.exec.ResultNotReadyException;
import tools.sqlclient.exec.ResultResponse;
import tools.sqlclient.remote.RemoteSqlClient;
import tools.sqlclient.util.OperationLog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * 面向 3 万规模元数据的刷新器，遵循 1000 行窗口与异步 SQL 协议。
 */
class MetadataRefreshService {
    private static final int WINDOW = 1000;
    private static final int MAX_OVERLOAD_RETRY = 3;
    private static final int MAX_EXPIRED_RETRY = 1;
    private static final long PER_BATCH_TIMEOUT_MS = Duration.ofSeconds(60).toMillis();
    private static final long POLL_INTERVAL_MIN_MS = 300;
    private static final long POLL_INTERVAL_MAX_MS = 500;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 8000;

    private final SQLiteManager sqliteManager;
    private final RemoteSqlClient remoteClient = new RemoteSqlClient();
    private final Object dbWriteLock;
    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
    private volatile String activeJobId;

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

    void cancelRefresh() {
        cancelRequested.set(true);
        String jobId = this.activeJobId;
        if (jobId != null) {
            try {
                remoteClient.cancelJob(jobId, "Cancelled by UI");
            } catch (Exception e) {
                OperationLog.log("取消远端元数据任务失败: " + e.getMessage());
            }
        }
    }

    RefreshStats refreshAll(String schema,
                            String dbUser,
                            Consumer<Progress> progressConsumer) throws Exception {
        cancelRequested.set(false);
        activeJobId = null;
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
                if (cancelRequested.get()) {
                    long cost = System.currentTimeMillis() - started;
                    OperationLog.log("元数据刷新已取消，未覆盖本地缓存");
                    return new RefreshStats(false, countTable(conn, "objects"), countTable(conn, "columns"), batches, "用户取消", cost);
                }
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
        batches += fetchPagedObjects(schema, dbUser, "table",
                """
                        SELECT table_schema AS schema_name, table_name AS object_name, 'table' AS object_type
                        FROM information_schema.tables
                        WHERE table_schema='%s' AND table_type='BASE TABLE'
                        ORDER BY table_schema, table_name
                        OFFSET %d LIMIT %d
                        """,
                conn, progressConsumer);
        batches += fetchPagedObjects(schema, dbUser, "view",
                """
                        SELECT table_schema AS schema_name, table_name AS object_name, 'view' AS object_type
                        FROM information_schema.views
                        WHERE table_schema='%s'
                        ORDER BY table_schema, table_name
                        OFFSET %d LIMIT %d
                        """,
                conn, progressConsumer);
        batches += fetchPagedObjects(schema, dbUser, "function",
                """
                        SELECT n.nspname AS schema_name, p.proname AS object_name, 'function' AS object_type
                        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                        WHERE n.nspname='%s' AND (p.prokind='f' OR p.prokind IS NULL AND p.proisagg = FALSE)
                        ORDER BY n.nspname, p.proname
                        OFFSET %d LIMIT %d
                        """,
                conn, progressConsumer);
        batches += fetchPagedObjects(schema, dbUser, "procedure",
                """
                        SELECT n.nspname AS schema_name, p.proname AS object_name, 'procedure' AS object_type
                        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                        WHERE n.nspname='%s' AND p.prokind='p'
                        ORDER BY n.nspname, p.proname
                        OFFSET %d LIMIT %d
                        """,
                conn, progressConsumer);
        return batches;
    }

    private int fetchPagedObjects(String schema,
                                  String dbUser,
                                  String type,
                                  String sqlTemplate,
                                  Connection conn,
                                  Consumer<Progress> progressConsumer) throws Exception {
        int batches = 0;
        for (int page = 0; ; page++) {
            ensureNotCancelled();
            String sql = String.format(sqlTemplate, schema, page * WINDOW, WINDOW);
            ResultResponse resp = executeOne(sql, dbUser, "meta:" + type, page);
            batches++;
            List<java.util.Map<String, String>> rows = Optional.ofNullable(resp.getRowMaps()).orElse(List.of());
            if (rows.isEmpty()) {
                break;
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR IGNORE INTO objects_staging(schema_name, object_name, object_type) VALUES(?,?,?)")) {
                for (java.util.Map<String, String> row : rows) {
                    String name = row.getOrDefault("object_name", null);
                    if (name != null && !name.isBlank()) {
                        ps.setString(1, schema);
                        ps.setString(2, name.trim());
                        ps.setString(3, type);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
            emitProgress(progressConsumer, "objects:" + type, batches, null, countTable(conn, "objects_staging"), countTable(conn, "columns_staging"));
            if (rows.size() < WINDOW) {
                break;
            }
        }
        return batches;
    }

    private int fetchColumns(String schema,
                             String dbUser,
                             Connection conn,
                             int totalObjects,
                             Consumer<Progress> progressConsumer) throws Exception {
        int batches = 0;
        for (int page = 0; ; page++) {
            ensureNotCancelled();
            String sql = """
                    SELECT table_schema AS schema_name, table_name AS object_name, column_name, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema='%s'
                    ORDER BY table_schema, table_name, ordinal_position
                    OFFSET %d LIMIT %d
                    """.formatted(schema, page * WINDOW, WINDOW);
            ResultResponse resp = executeOne(sql, dbUser, "meta:columns", page);
            batches++;
            List<java.util.Map<String, String>> rows = Optional.ofNullable(resp.getRowMaps()).orElse(List.of());
            if (rows.isEmpty()) {
                break;
            }
            try (PreparedStatement psIns = conn.prepareStatement(
                    "INSERT OR REPLACE INTO columns_staging(schema_name, object_name, column_name, sort_no) VALUES(?,?,?,?)")) {
                int idx = 0;
                for (java.util.Map<String, String> row : rows) {
                    idx++;
                    String colName = row.getOrDefault("column_name", null);
                    String objectName = row.getOrDefault("object_name", null);
                    if (colName == null || objectName == null) {
                        continue;
                    }
                    psIns.setString(1, schema);
                    psIns.setString(2, objectName.trim());
                    psIns.setString(3, colName.trim());
                    int sortNo = parseInt(row.get("ordinal_position"), idx);
                    psIns.setInt(4, sortNo);
                    psIns.addBatch();
                }
                psIns.executeBatch();
            }
            emitProgress(progressConsumer, "columns", batches, totalObjects, countTable(conn, "objects_staging"), countTable(conn, "columns_staging"));
            if (rows.size() < WINDOW) {
                break;
            }
        }
        return batches;
    }

    private ResultResponse executeOne(String sql, String dbUser, String label, int pageIndex) throws Exception {
        int overloadAttempts = 0;
        int expiredAttempts = 0;
        long backoff = INITIAL_BACKOFF_MS;
        while (true) {
            ensureNotCancelled();
            try {
                AsyncJobStatus submit = remoteClient.submitJob(sql, WINDOW, dbUser, label);
                activeJobId = submit.getJobId();
                long deadline = System.currentTimeMillis() + PER_BATCH_TIMEOUT_MS;
                ResultResponse ready = waitUntilReady(submit.getJobId(), label, pageIndex, deadline);
                ready.setJobId(submit.getJobId());
                logBatch(label, submit.getJobId(), pageIndex, ready);
                activeJobId = null;
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
                OperationLog.log("批次结果过期，自动重提一次 label=" + label + " page=" + pageIndex + " msg=" + re.getMessage());
            } finally {
                activeJobId = null;
            }
        }
    }

    private ResultResponse waitUntilReady(String jobId,
                                          String label,
                                          int pageIndex,
                                          long deadlineMs) throws Exception {
        ResultResponse latest = null;
        int statusPoll = 0;
        while (true) {
            try {
                latest = remoteClient.fetchResult(jobId, true, 1, WINDOW, null, null);
                boolean ready = Boolean.TRUE.equals(latest.getSuccess()) && (latest.getResultAvailable() == null || latest.getResultAvailable());
                boolean stillRunning = latest.getStatus() != null && Set.of("RUNNING", "QUEUED", "ACCEPTED", "CANCELLING")
                        .contains(latest.getStatus().toUpperCase(Locale.ROOT));
                if (latest.getArchiveError() != null && !latest.getArchiveError().isBlank()) {
                    throw new RuntimeException("结果归档失败: " + latest.getArchiveError());
                }
                if (ready && !stillRunning) {
                    return latest;
                }
            } catch (ResultNotReadyException ignored) {
                // continue polling
            } catch (ResultExpiredException re) {
                throw re;
            }
            if (System.currentTimeMillis() > deadlineMs) {
                String diag = "等待结果超时 label=" + label + " jobId=" + jobId + " page=" + pageIndex
                        + (latest != null && latest.getQueueDelayMillis() != null ? " queueDelayMillis=" + latest.getQueueDelayMillis() : "");
                throw new RuntimeException(diag);
            }
            if (++statusPoll % 3 == 0) {
                try {
                    AsyncJobStatus status = remoteClient.pollStatus(jobId);
                    if (status != null && status.getStatus() != null) {
                        OperationLog.log("[" + jobId + "] 状态 " + status.getStatus()
                                + (status.getProgressPercent() != null ? (" progress=" + status.getProgressPercent()) : "")
                                + (status.getQueueDelayMillis() != null ? (" queueDelayMillis=" + status.getQueueDelayMillis()) : ""));
                    }
                } catch (Exception ignored) {
                }
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

    private void ensureNotCancelled() {
        if (cancelRequested.get()) {
            throw new RuntimeException("元数据刷新已被用户取消");
        }
    }

    private void emitProgress(Consumer<Progress> consumer, String phase, int batches, Integer totalBatches, int objects, int columns) {
        if (consumer != null) {
            consumer.accept(new Progress(phase, batches, totalBatches, objects, columns));
        }
    }

    private void logBatch(String label, String jobId, int pageIndex, ResultResponse resp) {
        int batchSize = resp.getReturnedRowCount() != null ? resp.getReturnedRowCount() : Optional.ofNullable(resp.getRowMaps()).orElse(List.of()).size();
        OperationLog.log("[meta] label=" + label
                + " jobId=" + jobId
                + " page=" + pageIndex
                + " batchSize=" + batchSize
                + (resp.getQueueDelayMillis() != null ? (" queueDelayMillis=" + resp.getQueueDelayMillis()) : "")
                + (resp.getOverloaded() != null ? (" overloaded=" + resp.getOverloaded()) : "")
                + (resp.getExpiresAt() != null ? (" expiresAt=" + resp.getExpiresAt()) : "")
                + (resp.getLastAccessAt() != null ? (" lastAccessAt=" + resp.getLastAccessAt()) : "")
                + (resp.getCode() != null ? (" code=" + resp.getCode()) : "")
                + (resp.getNote() != null ? (" note=" + OperationLog.abbreviate(resp.getNote(), 80)) : "")
                + (resp.getThreadPool() != null ? (" threadPool=" + resp.getThreadPool()) : ""));
    }

    ResultResponse runSinglePage(String sql, String dbUser, String label) throws Exception {
        return executeOne(sql, dbUser, label, 0);
    }
}
