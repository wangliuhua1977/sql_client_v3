package tools.sqlclient.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 负责保存应用状态（例如上次打开的笔记列表）。
 */
public class AppStateRepository {
    private static final String OPEN_NOTES_KEY = "open_notes";
    private static final String FULL_WIDTH_KEY = "convert_full_width";
    private static final String CURRENT_STYLE_KEY = "editor_style";
    private static final String CURRENT_THEME_KEY = "app_theme";
    private final SQLiteManager sqliteManager;

    public AppStateRepository(SQLiteManager sqliteManager) {
        this.sqliteManager = sqliteManager;
    }

    public void saveOpenNotes(List<Long> noteIds) {
        String value = noteIds.stream().map(String::valueOf).collect(Collectors.joining(","));
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("INSERT INTO app_state(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")) {
            ps.setString(1, OPEN_NOTES_KEY);
            ps.setString(2, value);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("保存上次打开笔记失败", e);
        }
    }

    public List<Long> loadOpenNotes() {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT value FROM app_state WHERE key=?")) {
            ps.setString(1, OPEN_NOTES_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String value = rs.getString(1);
                    return parseIds(value);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("读取上次打开笔记失败", e);
        }
        return List.of();
    }

    public void saveBooleanOption(String key, boolean value) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("INSERT INTO app_state(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")) {
            ps.setString(1, key);
            ps.setString(2, value ? "1" : "0");
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("保存布尔配置失败", e);
        }
    }

    public boolean loadBooleanOption(String key, boolean defaultValue) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT value FROM app_state WHERE key=?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return "1".equals(rs.getString(1));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("读取布尔配置失败", e);
        }
        return defaultValue;
    }

    public boolean loadFullWidthOption(boolean defaultValue) {
        return loadBooleanOption(FULL_WIDTH_KEY, defaultValue);
    }

    public void saveFullWidthOption(boolean enabled) {
        saveBooleanOption(FULL_WIDTH_KEY, enabled);
    }

    public void saveCurrentStyleName(String styleName) {
        saveStringOption(CURRENT_STYLE_KEY, styleName);
    }

    public String loadCurrentStyleName(String defaultName) {
        return loadStringOption(CURRENT_STYLE_KEY, defaultName);
    }

    public void saveCurrentThemeName(String themeName) {
        saveStringOption(CURRENT_THEME_KEY, themeName);
    }

    public String loadCurrentThemeName(String defaultName) {
        return loadStringOption(CURRENT_THEME_KEY, defaultName);
    }

    public void saveStringOption(String key, String value) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("INSERT INTO app_state(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")) {
            ps.setString(1, key);
            ps.setString(2, value == null ? "" : value);
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("保存字符串配置失败", e);
        }
    }

    public String loadStringOption(String key, String defaultValue) {
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT value FROM app_state WHERE key=?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("读取字符串配置失败", e);
        }
        return defaultValue;
    }

    private List<Long> parseIds(String value) {
        if (value == null || value.isBlank()) return List.of();
        List<Long> ids = new ArrayList<>();
        Stream.of(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEach(s -> {
                    try { ids.add(Long.parseLong(s)); } catch (NumberFormatException ignored) {}
                });
        return ids;
    }
}
