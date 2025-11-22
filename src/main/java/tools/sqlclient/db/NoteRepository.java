package tools.sqlclient.db;

import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.Note;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 笔记存储，全部保存在本地 SQLite。
 */
public class NoteRepository {
    private final SQLiteManager sqliteManager;

    public NoteRepository(SQLiteManager sqliteManager) {
        this.sqliteManager = sqliteManager;
    }

    public Note create(String title, DatabaseType type) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO notes(title, content, db_type, updated_at) VALUES(?,?,?,?) RETURNING id")) {
            ps.setString(1, title);
            ps.setString(2, "");
            ps.setString(3, type.name());
            ps.setLong(4, now);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Note(rs.getLong(1), title, "", type, now);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("创建笔记失败", e);
        }
        throw new IllegalStateException("无法创建笔记");
    }

    public void updateContent(Note note, String content) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE notes SET content=?, db_type=?, updated_at=? WHERE id=?")) {
            ps.setString(1, content);
            ps.setString(2, note.getDatabaseType().name());
            ps.setLong(3, now);
            ps.setLong(4, note.getId());
            ps.executeUpdate();
            note.setContent(content);
            note.setUpdatedAt(now);
        } catch (Exception e) {
            throw new RuntimeException("更新笔记失败", e);
        }
    }

    public void rename(Note note, String newTitle) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE notes SET title=?, updated_at=? WHERE id=?")) {
            ps.setString(1, newTitle);
            ps.setLong(2, now);
            ps.setLong(3, note.getId());
            ps.executeUpdate();
            note.setTitle(newTitle);
            note.setUpdatedAt(now);
        } catch (Exception e) {
            throw new RuntimeException("重命名笔记失败", e);
        }
    }

    public Optional<Note> find(long id) {
        String sql = "SELECT id, title, content, db_type, updated_at FROM notes WHERE id=?";
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("查询笔记失败", e);
        }
        return Optional.empty();
    }

    public List<Note> listAll() {
        String sql = "SELECT id, title, content, db_type, updated_at FROM notes ORDER BY updated_at DESC, id DESC";
        List<Note> notes = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                notes.add(mapRow(rs));
            }
        } catch (Exception e) {
            throw new RuntimeException("获取笔记列表失败", e);
        }
        return notes;
    }

    private Note mapRow(ResultSet rs) throws Exception {
        return new Note(
                rs.getLong("id"),
                rs.getString("title"),
                rs.getString("content"),
                DatabaseType.valueOf(rs.getString("db_type")),
                rs.getLong("updated_at")
        );
    }
}
