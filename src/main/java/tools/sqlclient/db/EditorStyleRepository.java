package tools.sqlclient.db;

import tools.sqlclient.model.EditorStyle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class EditorStyleRepository {
    private final SQLiteManager sqliteManager;

    public EditorStyleRepository(SQLiteManager sqliteManager) {
        this.sqliteManager = sqliteManager;
    }

    public List<EditorStyle> listAll() {
        List<EditorStyle> styles = new ArrayList<>();
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT name, font_size, background, foreground, selection, caret, keyword, string_color, comment_color FROM editor_styles ORDER BY name ASC")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    styles.add(map(rs));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("读取编辑器样式失败", e);
        }
        return styles;
    }

    public void upsert(EditorStyle style) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("INSERT INTO editor_styles(name, font_size, background, foreground, selection, caret, keyword, string_color, comment_color) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET font_size=excluded.font_size, background=excluded.background, foreground=excluded.foreground, selection=excluded.selection, caret=excluded.caret, keyword=excluded.keyword, string_color=excluded.string_color, comment_color=excluded.comment_color")) {
            ps.setString(1, style.getName());
            ps.setInt(2, style.getFontSize());
            ps.setString(3, style.getBackground());
            ps.setString(4, style.getForeground());
            ps.setString(5, style.getSelection());
            ps.setString(6, style.getCaret());
            ps.setString(7, style.getKeyword());
            ps.setString(8, style.getStringColor());
            ps.setString(9, style.getCommentColor());
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("保存编辑器样式失败", e);
        }
    }

    private EditorStyle map(ResultSet rs) throws Exception {
        return new EditorStyle(
                rs.getString("name"),
                rs.getInt("font_size"),
                rs.getString("background"),
                rs.getString("foreground"),
                rs.getString("selection"),
                rs.getString("caret"),
                rs.getString("keyword"),
                rs.getString("string_color"),
                rs.getString("comment_color")
        );
    }
}
