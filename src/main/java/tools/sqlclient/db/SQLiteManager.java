package tools.sqlclient.db;

import org.sqlite.SQLiteConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 初始化本地 SQLite，包含 objects 和 columns 表。
 */
public class SQLiteManager {
    private final Path dbPath;

    public SQLiteManager(Path dbPath) {
        this.dbPath = dbPath;
    }

    public Connection getConnection() throws SQLException {
        SQLiteConfig config = new SQLiteConfig();
        // WAL + busy timeout 能够减少多线程写入时的锁冲突
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        config.setSynchronous(SQLiteConfig.SynchronousMode.NORMAL);
        config.setBusyTimeout(15000);
        return DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath(), config.toProperties());
    }

    public void initSchema() {
        try (Connection conn = getConnection(); Statement st = conn.createStatement()) {
            if (dbPath.getParent() != null) {
                Files.createDirectories(dbPath.getParent());
            }
            st.executeUpdate("CREATE TABLE IF NOT EXISTS objects (" +
                    "schema_name TEXT, object_name TEXT, object_type TEXT, " +
                    "use_count INTEGER DEFAULT 0, last_used_at INTEGER, " +
                    "PRIMARY KEY(schema_name, object_name))");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS columns (" +
                    "schema_name TEXT, object_name TEXT, column_name TEXT, sort_no INTEGER, " +
                    "use_count INTEGER DEFAULT 0, last_used_at INTEGER, " +
                    "PRIMARY KEY(schema_name, object_name, column_name))");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS notes (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "title TEXT NOT NULL, " +
                    "content TEXT, " +
                    "db_type TEXT NOT NULL, " +
                    "created_at INTEGER NOT NULL, " +
                    "updated_at INTEGER NOT NULL, " +
                    "tags TEXT DEFAULT '', " +
                    "starred INTEGER DEFAULT 0"
                    + ")");
            // 兼容旧版本：逐列补齐，避免缺失 created_at/updated_at 等字段导致查询失败
            ensureColumn(conn, "notes", "created_at", "INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
            ensureColumn(conn, "notes", "updated_at", "INTEGER NOT NULL DEFAULT (strftime('%s','now')*1000)");
            ensureColumn(conn, "notes", "tags", "TEXT DEFAULT ''");
            ensureColumn(conn, "notes", "starred", "INTEGER DEFAULT 0");
            ensureUniqueIndex(conn, "idx_notes_title_unique", "notes", "title");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS app_state (" +
                    "key TEXT PRIMARY KEY, " +
                    "value TEXT"
                    + ")");
        } catch (Exception e) {
            throw new RuntimeException("初始化 SQLite 失败", e);
        }
    }

    private void ensureColumn(Connection conn, String table, String column, String definition) throws SQLException {
        if (!columnExists(conn, table, column)) {
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
            }
        }
    }

    private void ensureUniqueIndex(Connection conn, String indexName, String table, String column) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS " + indexName + " ON " + table + "(" + column + ")");
        }
    }

    private boolean columnExists(Connection conn, String table, String column) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info(" + table + ")");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                if (column.equalsIgnoreCase(rs.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }
}
