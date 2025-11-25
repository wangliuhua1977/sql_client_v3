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
             PreparedStatement ps = conn.prepareStatement("SELECT name, font_size, background, foreground, selection, caret, keyword, string_color, comment_color, number_color, operator_color, function_color, datatype_color, identifier_color, literal_color, line_highlight, bracket_color FROM editor_styles ORDER BY name ASC")) {
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
             PreparedStatement ps = conn.prepareStatement("INSERT INTO editor_styles(name, font_size, background, foreground, selection, caret, keyword, string_color, comment_color, number_color, operator_color, function_color, datatype_color, identifier_color, literal_color, line_highlight, bracket_color) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET font_size=excluded.font_size, background=excluded.background, foreground=excluded.foreground, selection=excluded.selection, caret=excluded.caret, keyword=excluded.keyword, string_color=excluded.string_color, comment_color=excluded.comment_color, number_color=excluded.number_color, operator_color=excluded.operator_color, function_color=excluded.function_color, datatype_color=excluded.datatype_color, identifier_color=excluded.identifier_color, literal_color=excluded.literal_color, line_highlight=excluded.line_highlight, bracket_color=excluded.bracket_color")) {
            ps.setString(1, style.getName());
            ps.setInt(2, style.getFontSize());
            ps.setString(3, style.getBackground());
            ps.setString(4, style.getForeground());
            ps.setString(5, style.getSelection());
            ps.setString(6, style.getCaret());
            ps.setString(7, style.getKeyword());
            ps.setString(8, style.getStringColor());
            ps.setString(9, style.getCommentColor());
            ps.setString(10, style.getNumberColor());
            ps.setString(11, style.getOperatorColor());
            ps.setString(12, style.getFunctionColor());
            ps.setString(13, style.getDataTypeColor());
            ps.setString(14, style.getIdentifierColor());
            ps.setString(15, style.getLiteralColor());
            ps.setString(16, style.getLineHighlight());
            ps.setString(17, style.getBracketColor());
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("保存编辑器样式失败", e);
        }
    }

    private EditorStyle map(ResultSet rs) throws Exception {
        String bg = coalesce(rs.getString("background"), "#FFFFFF");
        String fg = coalesce(rs.getString("foreground"), "#000000");
        String sel = coalesce(rs.getString("selection"), "#CCE8FF");
        String caret = coalesce(rs.getString("caret"), "#000000");
        String kw = coalesce(rs.getString("keyword"), "#005CC5");
        String str = coalesce(rs.getString("string_color"), "#032F62");
        String com = coalesce(rs.getString("comment_color"), "#6A737D");
        String num = coalesce(rs.getString("number_color"), "#1C7C54");
        String op = coalesce(rs.getString("operator_color"), "#000000");
        String func = coalesce(rs.getString("function_color"), "#6F42C1");
        String dt = coalesce(rs.getString("datatype_color"), "#005CC5");
        String ident = coalesce(rs.getString("identifier_color"), "#24292E");
        String lit = coalesce(rs.getString("literal_color"), "#D73A49");
        String line = coalesce(rs.getString("line_highlight"), "#F6F8FA");
        String br = coalesce(rs.getString("bracket_color"), "#FFDD88");
        return new EditorStyle(
                rs.getString("name"),
                rs.getInt("font_size"),
                bg, fg, sel, caret, kw, str, com, num, op, func, dt, ident, lit, line, br
        );
    }

    private String coalesce(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
