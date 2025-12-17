package tools.sqlclient.db;

import org.sqlite.SQLiteConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

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

    public Path getDbPath() {
        return dbPath;
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
            st.executeUpdate("CREATE TABLE IF NOT EXISTS meta_snapshot (" +
                    "meta_type TEXT PRIMARY KEY, " +
                    "meta_hash TEXT NOT NULL, " +
                    "updated_at INTEGER NOT NULL)"
            );
            st.executeUpdate("CREATE TABLE IF NOT EXISTS notes (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "title TEXT NOT NULL, " +
                    "content TEXT, " +
                    "db_type TEXT NOT NULL, " +
                    "created_at INTEGER NOT NULL, " +
                    "updated_at INTEGER NOT NULL, " +
                    "tags TEXT DEFAULT '', " +
                    "starred INTEGER DEFAULT 0, " +
                    "trashed INTEGER DEFAULT 0, " +
                    "deleted_at INTEGER DEFAULT 0, " +
                    "style_name TEXT DEFAULT ''"
                    + ")");
            // 兼容旧版本：逐列补齐，避免缺失 created_at/updated_at 等字段导致查询失败
            // SQLite 不允许使用表达式默认值在 ALTER TABLE 里添加列，这里使用常量默认值并由上层插入逻辑填充实际时间戳
            ensureColumn(conn, "notes", "created_at", "INTEGER NOT NULL DEFAULT 0");
            ensureColumn(conn, "notes", "updated_at", "INTEGER NOT NULL DEFAULT 0");
            ensureColumn(conn, "notes", "tags", "TEXT DEFAULT ''");
            ensureColumn(conn, "notes", "starred", "INTEGER DEFAULT 0");
            ensureColumn(conn, "notes", "trashed", "INTEGER DEFAULT 0");
            ensureColumn(conn, "notes", "deleted_at", "INTEGER DEFAULT 0");
            ensureColumn(conn, "notes", "style_name", "TEXT DEFAULT ''");
            normalizeNoteTitles(conn);
            ensureUniqueIndex(conn, "idx_notes_title_unique", "notes", "title");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS note_links (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "from_note_id INTEGER NOT NULL, " +
                    "to_note_id INTEGER NOT NULL, " +
                    "created_at INTEGER NOT NULL, " +
                    "last_seen_at INTEGER NOT NULL, " +
                    "UNIQUE(from_note_id, to_note_id)" +
                    ")");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS app_state (" +
                    "key TEXT PRIMARY KEY, " +
                    "value TEXT"
                    + ")");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS sql_snippets (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "name TEXT NOT NULL, " +
                    "tags TEXT DEFAULT '', " +
                    "content TEXT NOT NULL, " +
                    "created_at INTEGER NOT NULL DEFAULT 0, " +
                    "updated_at INTEGER NOT NULL DEFAULT 0" +
                    ")");
            ensureUniqueIndex(conn, "idx_sql_snippets_name", "sql_snippets", "name");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS sql_history (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "sql_text TEXT NOT NULL UNIQUE, " +
                    "db_type TEXT, " +
                    "created_at INTEGER NOT NULL DEFAULT 0, " +
                    "last_executed_at INTEGER NOT NULL DEFAULT 0" +
                    ")");
            ensureColumn(conn, "sql_history", "db_type", "TEXT");
            ensureColumn(conn, "sql_history", "created_at", "INTEGER NOT NULL DEFAULT 0");
            ensureColumn(conn, "sql_history", "last_executed_at", "INTEGER NOT NULL DEFAULT 0");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS editor_styles(" +
                    "name TEXT PRIMARY KEY, " +
                    "font_size INTEGER NOT NULL DEFAULT 14, " +
                    "background TEXT DEFAULT '#FFFFFF', " +
                    "foreground TEXT DEFAULT '#000000', " +
                    "selection TEXT DEFAULT '#CCE8FF', " +
                    "caret TEXT DEFAULT '#000000', " +
                    "keyword TEXT DEFAULT '#005CC5', " +
                    "string_color TEXT DEFAULT '#032F62', " +
                    "comment_color TEXT DEFAULT '#6A737D', " +
                    "number_color TEXT DEFAULT '#1C7C54', " +
                    "operator_color TEXT DEFAULT '#000000', " +
                    "function_color TEXT DEFAULT '#6F42C1', " +
                    "datatype_color TEXT DEFAULT '#005CC5', " +
                    "identifier_color TEXT DEFAULT '#24292E', " +
                    "literal_color TEXT DEFAULT '#D73A49', " +
                    "line_highlight TEXT DEFAULT '#F6F8FA', " +
                    "bracket_color TEXT DEFAULT '#FFDD88'"
                    + ")");
            ensureColumn(conn, "editor_styles", "number_color", "TEXT DEFAULT '#1C7C54'");
            ensureColumn(conn, "editor_styles", "operator_color", "TEXT DEFAULT '#000000'");
            ensureColumn(conn, "editor_styles", "function_color", "TEXT DEFAULT '#6F42C1'");
            ensureColumn(conn, "editor_styles", "datatype_color", "TEXT DEFAULT '#005CC5'");
            ensureColumn(conn, "editor_styles", "identifier_color", "TEXT DEFAULT '#24292E'");
            ensureColumn(conn, "editor_styles", "literal_color", "TEXT DEFAULT '#D73A49'");
            ensureColumn(conn, "editor_styles", "line_highlight", "TEXT DEFAULT '#F6F8FA'");
            ensureColumn(conn, "editor_styles", "bracket_color", "TEXT DEFAULT '#FFDD88'");
            ensureDefaultStyle(conn);
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

    /**
     * 全量扫描 notes，确保标题全局唯一：
     * 1) 按 title、created_at、id 排序，第一次出现的标题保留；后续同名自动追加 “(副本 n)”
     * 2) 通过内存 Set 判断是否重复，始终生成未使用的新标题，避免 UNIQUE 约束创建失败
     */
    private void normalizeNoteTitles(Connection conn) throws SQLException {
        boolean autoCommit = conn.getAutoCommit();
        Savepoint sp = null;
        try {
            conn.setAutoCommit(false);
            sp = conn.setSavepoint("normalize_notes");
            Set<String> seenTitles = new HashSet<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, title FROM notes ORDER BY title ASC, created_at ASC, id ASC");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("id");
                    String title = rs.getString("title");
                    if (title == null) {
                        title = "(未命名)";
                    }
                    if (seenTitles.add(title)) {
                        continue; // 首次出现
                    }
                    int copyIndex = 1;
                    String candidate;
                    do {
                        candidate = title + " (副本 " + copyIndex + ")";
                        copyIndex++;
                    } while (seenTitles.contains(candidate));
                    seenTitles.add(candidate);
                    try (PreparedStatement upd = conn.prepareStatement(
                            "UPDATE notes SET title = ?, updated_at = ? WHERE id = ?")) {
                        upd.setString(1, candidate);
                        upd.setLong(2, System.currentTimeMillis());
                        upd.setLong(3, id);
                        upd.executeUpdate();
                    }
                }
            }
            conn.commit();
        } catch (SQLException ex) {
            if (sp != null) {
                try {
                    conn.rollback(sp);
                } catch (SQLException ignore) {
                    // ignore rollback failure
                }
            }
            throw ex;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    private void ensureDefaultStyle(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(1) FROM editor_styles")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) == 0) {
                    try (PreparedStatement ins = conn.prepareStatement(
                            "INSERT INTO editor_styles(name, font_size, background, foreground, selection, caret, keyword, string_color, comment_color, " +
                                    "number_color, operator_color, function_color, datatype_color, identifier_color, literal_color, line_highlight, bracket_color) " +
                                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                        ins.setString(1, "默认");
                        ins.setInt(2, 14);
                        ins.setString(3, "#FFFFFF");
                        ins.setString(4, "#000000");
                        ins.setString(5, "#CCE8FF");
                        ins.setString(6, "#000000");
                        ins.setString(7, "#005CC5");
                        ins.setString(8, "#032F62");
                        ins.setString(9, "#6A737D");
                        ins.setString(10, "#1C7C54");
                        ins.setString(11, "#000000");
                        ins.setString(12, "#6F42C1");
                        ins.setString(13, "#005CC5");
                        ins.setString(14, "#24292E");
                        ins.setString(15, "#D73A49");
                        ins.setString(16, "#F6F8FA");
                        ins.setString(17, "#FFDD88");
                        ins.executeUpdate();
                    }
                }
            }
        }
    }
}
