package tools.sqlclient.db;

import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.SqlHistoryEntry;
import tools.sqlclient.util.OperationLog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SqlHistoryRepository {
    private final SQLiteManager sqliteManager;

    public SqlHistoryRepository(SQLiteManager sqliteManager) {
        this.sqliteManager = sqliteManager;
    }

    public void recordExecution(String sqlText, DatabaseType dbType) {
        String normalized = sqlText == null ? "" : sqlText.trim();
        if (normalized.isEmpty()) {
            return;
        }
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection()) {
            try (PreparedStatement update = conn.prepareStatement(
                    "UPDATE sql_history SET last_executed_at=?, db_type=? WHERE sql_text=?")) {
                update.setLong(1, now);
                update.setString(2, dbType == null ? null : dbType.name());
                update.setString(3, normalized);
                int rows = update.executeUpdate();
                if (rows > 0) {
                    return;
                }
            }
            try (PreparedStatement insert = conn.prepareStatement(
                    "INSERT INTO sql_history(sql_text, db_type, created_at, last_executed_at) VALUES(?,?,?,?)")) {
                insert.setString(1, normalized);
                insert.setString(2, dbType == null ? null : dbType.name());
                insert.setLong(3, now);
                insert.setLong(4, now);
                insert.executeUpdate();
            }
        } catch (Exception e) {
            OperationLog.log("记录执行历史失败: " + e.getMessage());
        }
    }

    public List<SqlHistoryEntry> listAll() {
        String sql = "SELECT id, sql_text, db_type, created_at, last_executed_at FROM sql_history " +
                "ORDER BY last_executed_at DESC, id DESC";
        List<SqlHistoryEntry> list = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(mapRow(rs));
            }
        } catch (Exception e) {
            throw new RuntimeException("读取执行历史失败", e);
        }
        return list;
    }

    public void delete(long id) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM sql_history WHERE id=?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("删除执行历史失败", e);
        }
    }

    public void clearAll() {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM sql_history")) {
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("清空执行历史失败", e);
        }
    }

    private SqlHistoryEntry mapRow(ResultSet rs) throws Exception {
        String dbType = rs.getString("db_type");
        return new SqlHistoryEntry(
                rs.getLong("id"),
                rs.getString("sql_text"),
                dbType == null ? null : DatabaseType.valueOf(dbType),
                rs.getLong("created_at"),
                rs.getLong("last_executed_at")
        );
    }
}
