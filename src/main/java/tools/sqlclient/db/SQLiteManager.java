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
            // SQLite 不允许使用表达式默认值在 ALTER TABLE 里添加列，这里使用常量默认值并由上层插入逻辑填充实际时间戳
            ensureColumn(conn, "notes", "created_at", "INTEGER NOT NULL DEFAULT 0");
            ensureColumn(conn, "notes", "updated_at", "INTEGER NOT NULL DEFAULT 0");
            ensureColumn(conn, "notes", "tags", "TEXT DEFAULT ''");
            ensureColumn(conn, "notes", "starred", "INTEGER DEFAULT 0");
            deduplicateNoteTitles(conn);
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

    /**
     * 在补充唯一索引前处理已有重复标题，避免创建索引时抛出 UNIQUE 约束异常。
     * 规则：保留同名笔记中的最早一条，其他追加 “(副本 n)” 后缀直到唯一。
     */
    private void deduplicateNoteTitles(Connection conn) throws SQLException {
        // 查找重复标题
        try (PreparedStatement dupStmt = conn.prepareStatement(
                "SELECT title FROM notes GROUP BY title HAVING COUNT(*) > 1");
             ResultSet dupRs = dupStmt.executeQuery()) {
            while (dupRs.next()) {
                String title = dupRs.getString("title");
                resolveDuplicatesForTitle(conn, title);
            }
        }
    }

    private void resolveDuplicatesForTitle(Connection conn, String title) throws SQLException {
        // 按创建时间和 ID 排序，保留第一条，其余重命名
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM notes WHERE title = ? ORDER BY created_at ASC, id ASC")) {
            ps.setString(1, title);
            try (ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                int copyIndex = 1;
                while (rs.next()) {
                    long id = rs.getLong("id");
                    if (first) {
                        first = false;
                        continue; // 保留第一条
                    }
                    String newTitle;
                    do {
                        newTitle = title + " (副本 " + copyIndex + ")";
                        copyIndex++;
                    } while (titleExists(conn, newTitle));
                    try (PreparedStatement upd = conn.prepareStatement(
                            "UPDATE notes SET title = ?, updated_at = ? WHERE id = ?")) {
                        upd.setString(1, newTitle);
                        upd.setLong(2, System.currentTimeMillis());
                        upd.setLong(3, id);
                        upd.executeUpdate();
                    }
                }
            }
        }
    }

    private boolean titleExists(Connection conn, String title) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM notes WHERE title = ? LIMIT 1")) {
            ps.setString(1, title);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
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
