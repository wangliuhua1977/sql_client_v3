package tools.sqlclient.db;

import org.sqlite.SQLiteConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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
        config.setBusyTimeout(5000);
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
        } catch (Exception e) {
            throw new RuntimeException("初始化 SQLite 失败", e);
        }
    }
}
