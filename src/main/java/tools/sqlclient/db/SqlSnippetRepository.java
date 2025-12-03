package tools.sqlclient.db;

import tools.sqlclient.model.SqlSnippet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SQL 片段仓库，负责持久化到本地 SQLite。
 */
public class SqlSnippetRepository {
    private final SQLiteManager sqliteManager;

    public SqlSnippetRepository(SQLiteManager sqliteManager) {
        this.sqliteManager = sqliteManager;
    }

    public SqlSnippet create(String name, String tags, String content) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO sql_snippets(name, tags, content, created_at, updated_at) VALUES(?,?,?,?,?) RETURNING id")) {
            ps.setString(1, name);
            ps.setString(2, tags == null ? "" : tags);
            ps.setString(3, content == null ? "" : content);
            ps.setLong(4, now);
            ps.setLong(5, now);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new SqlSnippet(rs.getLong(1), name, tags == null ? "" : tags, content == null ? "" : content, now, now);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("创建 SQL 片段失败", e);
        }
        throw new IllegalStateException("无法创建 SQL 片段");
    }

    public void update(SqlSnippet snippet) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE sql_snippets SET name=?, tags=?, content=?, updated_at=? WHERE id=?")) {
            ps.setString(1, snippet.getName());
            ps.setString(2, snippet.getTags() == null ? "" : snippet.getTags());
            ps.setString(3, snippet.getContent() == null ? "" : snippet.getContent());
            ps.setLong(4, now);
            ps.setLong(5, snippet.getId());
            ps.executeUpdate();
            snippet.setUpdatedAt(now);
        } catch (Exception e) {
            throw new RuntimeException("更新 SQL 片段失败", e);
        }
    }

    public void delete(long id) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM sql_snippets WHERE id=?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("删除 SQL 片段失败", e);
        }
    }

    public List<SqlSnippet> listAll() {
        String sql = "SELECT id, name, tags, content, created_at, updated_at FROM sql_snippets ORDER BY updated_at DESC, id DESC";
        List<SqlSnippet> list = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(mapRow(rs));
            }
        } catch (Exception e) {
            throw new RuntimeException("查询 SQL 片段失败", e);
        }
        return list;
    }

    public List<SqlSnippet> search(String keyword) {
        String sql = "SELECT id, name, tags, content, created_at, updated_at FROM sql_snippets WHERE name LIKE ? OR tags LIKE ? OR content LIKE ? ORDER BY updated_at DESC, id DESC";
        String kw = "%" + (keyword == null ? "" : keyword.trim()) + "%";
        List<SqlSnippet> list = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, kw);
            ps.setString(2, kw);
            ps.setString(3, kw);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("搜索 SQL 片段失败", e);
        }
        return list;
    }

    public Optional<SqlSnippet> find(long id) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT id, name, tags, content, created_at, updated_at FROM sql_snippets WHERE id=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("查询 SQL 片段失败", e);
        }
        return Optional.empty();
    }

    private SqlSnippet mapRow(ResultSet rs) throws Exception {
        return new SqlSnippet(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("tags"),
                rs.getString("content"),
                rs.getLong("created_at"),
                rs.getLong("updated_at")
        );
    }
}
