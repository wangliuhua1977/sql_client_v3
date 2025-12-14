package tools.sqlclient.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.Note;
import tools.sqlclient.util.OperationLog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 笔记存储，全部保存在本地 SQLite。
 */
public class NoteRepository {
    private static final String BASE_COLUMNS = "id, title, content, db_type, created_at, updated_at, tags, starred, trashed, deleted_at, style_name";
    private static final Logger log = LoggerFactory.getLogger(NoteRepository.class);
    private final SQLiteManager sqliteManager;

    public NoteRepository(SQLiteManager sqliteManager) {
        this.sqliteManager = sqliteManager;
    }

    public Note create(String title, DatabaseType type) {
        if (titleExists(title, null)) {
            throw new IllegalArgumentException("笔记标题已存在，请更换名称");
        }
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO notes(title, content, db_type, created_at, updated_at, tags, starred, trashed, deleted_at, style_name) VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING id")) {
            ps.setString(1, title);
            ps.setString(2, "");
            ps.setString(3, type.name());
            ps.setLong(4, now);
            ps.setLong(5, now);
            ps.setString(6, "");
            ps.setInt(7, 0);
            ps.setInt(8, 0);
            ps.setLong(9, 0);
            ps.setString(10, "");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Note(rs.getLong(1), title, "", type, now, now, "", false, false, 0, "");
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
        if (titleExists(newTitle, note.getId())) {
            throw new IllegalArgumentException("笔记标题已存在，请更换名称");
        }
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

    public void updateMetadata(Note note, String tags, boolean starred) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE notes SET tags=?, starred=?, updated_at=? WHERE id=?")) {
            ps.setString(1, tags);
            ps.setInt(2, starred ? 1 : 0);
            ps.setLong(3, now);
            ps.setLong(4, note.getId());
            ps.executeUpdate();
            note.setTags(tags);
            note.setStarred(starred);
            note.setUpdatedAt(now);
        } catch (Exception e) {
            throw new RuntimeException("更新笔记标签/星标失败", e);
        }
    }

    public boolean titleExists(String title, Long excludeId) {
        String sql = "SELECT 1 FROM notes WHERE title=?" + (excludeId != null ? " AND id<>?" : "") + " LIMIT 1";
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, title);
            if (excludeId != null) {
                ps.setLong(2, excludeId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (Exception e) {
            throw new RuntimeException("检查笔记标题失败", e);
        }
    }

    public Optional<Note> find(long id) {
        String sql = "SELECT " + BASE_COLUMNS + " FROM notes WHERE id=?";
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
        String sql = "SELECT " + BASE_COLUMNS + " FROM notes ORDER BY updated_at DESC, id DESC";
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

    public List<Note> listByTrashed(boolean trashed) {
        String sql = "SELECT " + BASE_COLUMNS + " FROM notes WHERE trashed=? ORDER BY updated_at DESC, id DESC";
        List<Note> notes = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, trashed ? 1 : 0);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    notes.add(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("获取笔记列表失败", e);
        }
        return notes;
    }

    public List<Note> listByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        String placeholders = ids.stream().map(i -> "?").collect(java.util.stream.Collectors.joining(","));
        String sql = "SELECT " + BASE_COLUMNS + " FROM notes WHERE id IN (" + placeholders + ") ORDER BY updated_at DESC";
        List<Note> notes = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Long id : ids) {
                ps.setLong(idx++, id);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    notes.add(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("按 ID 获取笔记失败", e);
        }
        return notes;
    }

    public Note findOrCreateByTitle(String title) {
        if (title == null || title.isBlank()) {
            throw new IllegalArgumentException("标题不能为空");
        }
        String normalized = title.trim();
        String sql = "SELECT " + BASE_COLUMNS + " FROM notes WHERE lower(title)=lower(?) ORDER BY updated_at DESC, id DESC LIMIT 1";
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, normalized);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapRow(rs);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("查找笔记失败", e);
        }
        // 不存在则创建占位笔记
        OperationLog.log("创建占位笔记: " + normalized);
        return create(normalized, DatabaseType.POSTGRESQL);
    }

    public void updateNoteLinks(long fromNoteId, Set<Long> toNoteIds) {
        long now = Instant.now().toEpochMilli();
        if (toNoteIds == null) {
            toNoteIds = Set.of();
        }
        try (Connection conn = sqliteManager.getConnection()) {
            conn.setAutoCommit(false);
            java.util.Set<Long> existing = new java.util.HashSet<>();
            try (PreparedStatement ps = conn.prepareStatement("SELECT to_note_id FROM note_links WHERE from_note_id=?")) {
                ps.setLong(1, fromNoteId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        existing.add(rs.getLong(1));
                    }
                }
            }

            // 更新已存在的 last_seen_at
            try (PreparedStatement ps = conn.prepareStatement("UPDATE note_links SET last_seen_at=? WHERE from_note_id=? AND to_note_id=?")) {
                for (Long id : toNoteIds) {
                    if (existing.contains(id)) {
                        ps.setLong(1, now);
                        ps.setLong(2, fromNoteId);
                        ps.setLong(3, id);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }

            // 插入新的
            try (PreparedStatement ps = conn.prepareStatement("INSERT OR IGNORE INTO note_links(from_note_id, to_note_id, created_at, last_seen_at) VALUES(?,?,?,?)")) {
                for (Long id : toNoteIds) {
                    if (!existing.contains(id)) {
                        ps.setLong(1, fromNoteId);
                        ps.setLong(2, id);
                        ps.setLong(3, now);
                        ps.setLong(4, now);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }

            // 删除已移除的
            if (!existing.isEmpty()) {
                Set<Long> finalToNoteIds = toNoteIds;
                List<Long> toDelete = existing.stream()
                        .filter(old -> !finalToNoteIds.contains(old))
                        .collect(Collectors.toList());
                if (!toDelete.isEmpty()) {
                    String placeholders = toDelete.stream().map(x -> "?").collect(Collectors.joining(","));
                    String delSql = "DELETE FROM note_links WHERE from_note_id=? AND to_note_id IN (" + placeholders + ")";
                    try (PreparedStatement del = conn.prepareStatement(delSql)) {
                        del.setLong(1, fromNoteId);
                        int idx = 2;
                        for (Long id : toDelete) {
                            del.setLong(idx++, id);
                        }
                        del.executeUpdate();
                    }
                }
            }

            conn.commit();
            log.debug("已同步笔记引用关系 from {} -> {}", fromNoteId, toNoteIds);
        } catch (Exception e) {
            throw new RuntimeException("更新笔记链接失败", e);
        }
    }

    public List<Note> findBacklinks(long noteId) {
        String sql = "SELECT n.id, n.title, n.content, n.db_type, n.created_at, n.updated_at, n.tags, n.starred, n.trashed, n.deleted_at " +
                "FROM note_links l JOIN notes n ON l.from_note_id = n.id WHERE l.to_note_id=? ORDER BY n.updated_at DESC, n.id DESC";
        List<Note> notes = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, noteId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    notes.add(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("查询反向链接失败", e);
        }
        return notes;
    }

    public List<Note> search(String keyword, boolean fullText, boolean includeTrash) {
        StringBuilder sql = new StringBuilder("SELECT " + BASE_COLUMNS + " FROM notes WHERE 1=1");
        List<String> params = new ArrayList<>();
        if (!includeTrash) {
            sql.append(" AND trashed=0");
        }
        if (keyword != null && !keyword.isBlank()) {
            String kw = "%" + keyword.trim() + "%";
            if (fullText) {
                sql.append(" AND (title LIKE ? OR tags LIKE ? OR content LIKE ?)");
                params.add(kw);
                params.add(kw);
                params.add(kw);
            } else {
                sql.append(" AND (title LIKE ? OR tags LIKE ?)");
                params.add(kw);
                params.add(kw);
            }
        }
        sql.append(" ORDER BY starred DESC, updated_at DESC, id DESC");
        List<Note> notes = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (String p : params) {
                ps.setString(idx++, p);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    notes.add(mapRow(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("搜索笔记失败", e);
        }
        return notes;
    }

    private Note mapRow(ResultSet rs) throws Exception {
        return new Note(
                rs.getLong("id"),
                rs.getString("title"),
                rs.getString("content"),
                DatabaseType.valueOf(rs.getString("db_type")),
                rs.getLong("created_at"),
                rs.getLong("updated_at"),
                rs.getString("tags"),
                rs.getInt("starred") == 1,
                rs.getInt("trashed") == 1,
                rs.getLong("deleted_at"),
                rs.getString("style_name")
        );
    }

    public void updateStyleName(Note note, String styleName) {
        long now = Instant.now().toEpochMilli();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE notes SET style_name=?, updated_at=? WHERE id=?")) {
            ps.setString(1, styleName == null ? "" : styleName);
            ps.setLong(2, now);
            ps.setLong(3, note.getId());
            ps.executeUpdate();
            note.setStyleName(styleName);
            note.setUpdatedAt(now);
        } catch (Exception e) {
            throw new RuntimeException("保存笔记样式失败", e);
        }
    }

    public void moveToTrash(Note note) {
        long now = Instant.now().toEpochMilli();
        String trashedTitle = ensureTrashSuffix(note.getTitle(), now, note.getId());
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE notes SET trashed=1, deleted_at=?, updated_at=?, title=? WHERE id=?")) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setString(3, trashedTitle);
            ps.setLong(4, note.getId());
            ps.executeUpdate();
            note.setTrashed(true);
            note.setDeletedAt(now);
            note.setUpdatedAt(now);
            note.setTitle(trashedTitle);
        } catch (Exception e) {
            throw new RuntimeException("移动到垃圾箱失败", e);
        }
    }

    public void restore(Note note) {
        long now = Instant.now().toEpochMilli();
        String targetTitle = stripTrashSuffix(note.getTitle());
        boolean conflict = titleExists(targetTitle, note.getId());
        if (conflict) {
            targetTitle = ensureTrashSuffix(targetTitle, note.getDeletedAt() > 0 ? note.getDeletedAt() : now, note.getId());
        }
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE notes SET trashed=0, deleted_at=0, updated_at=?, title=? WHERE id=?")) {
            ps.setLong(1, now);
            ps.setString(2, targetTitle);
            ps.setLong(3, note.getId());
            ps.executeUpdate();
            note.setTrashed(false);
            note.setDeletedAt(0);
            note.setUpdatedAt(now);
            note.setTitle(targetTitle);
        } catch (Exception e) {
            throw new RuntimeException("恢复笔记失败", e);
        }
    }

    public void deleteForever(Note note) {
        if (note == null || !note.isTrashed()) return;
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM notes WHERE id=?")) {
            ps.setLong(1, note.getId());
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("彻底删除笔记失败", e);
        }
    }

    public void emptyTrash() {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM notes WHERE trashed=1")) {
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("清空垃圾箱失败", e);
        }
    }

    private String ensureTrashSuffix(String title, long timestamp, long id) {
        String base = stripTrashSuffix(title);
        long marker = timestamp > 0 ? timestamp : id;
        return base + " [trash-" + marker + "]";
    }

    private String stripTrashSuffix(String title) {
        if (title == null) return "";
        return title.replaceFirst("\\s*\\[trash-\\d+\\]$", "");
    }
}
