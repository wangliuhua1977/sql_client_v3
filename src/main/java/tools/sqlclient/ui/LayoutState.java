package tools.sqlclient.ui;

import java.awt.*;
import java.util.prefs.Preferences;

/**
 * 负责布局状态持久化与恢复。
 */
public class LayoutState {
    private static final String KEY_LEFT_DIVIDER = "layout.left.divider";
    private static final String KEY_RIGHT_DIVIDER = "layout.right.divider";
    private static final String KEY_BOTTOM_DIVIDER = "layout.bottom.divider";
    private static final String KEY_LEFT_VISIBLE = "layout.left.visible";
    private static final String KEY_RIGHT_VISIBLE = "layout.right.visible";
    private static final String KEY_BOTTOM_VISIBLE = "layout.bottom.visible";
    private static final String KEY_WINDOW_X = "layout.window.x";
    private static final String KEY_WINDOW_Y = "layout.window.y";
    private static final String KEY_WINDOW_W = "layout.window.w";
    private static final String KEY_WINDOW_H = "layout.window.h";
    private static final String KEY_LOG_DOCK = "layout.log.dock";
    private static final String KEY_LOG_FLOAT_BOUNDS = "layout.log.float";
    private static final String KEY_EDITOR_MAX = "layout.editor.max";

    private final Preferences prefs = Preferences.userNodeForPackage(LayoutState.class);

    public int getLeftDivider(int defaultValue) {
        return prefs.getInt(KEY_LEFT_DIVIDER, defaultValue);
    }

    public void setLeftDivider(int value) {
        prefs.putInt(KEY_LEFT_DIVIDER, value);
    }

    public int getRightDivider(int defaultValue) {
        return prefs.getInt(KEY_RIGHT_DIVIDER, defaultValue);
    }

    public void setRightDivider(int value) {
        prefs.putInt(KEY_RIGHT_DIVIDER, value);
    }

    public int getBottomDivider(int defaultValue) {
        return prefs.getInt(KEY_BOTTOM_DIVIDER, defaultValue);
    }

    public void setBottomDivider(int value) {
        prefs.putInt(KEY_BOTTOM_DIVIDER, value);
    }

    public boolean isLeftVisible(boolean defaultValue) {
        return prefs.getBoolean(KEY_LEFT_VISIBLE, defaultValue);
    }

    public void setLeftVisible(boolean visible) {
        prefs.putBoolean(KEY_LEFT_VISIBLE, visible);
    }

    public boolean isRightVisible(boolean defaultValue) {
        return prefs.getBoolean(KEY_RIGHT_VISIBLE, defaultValue);
    }

    public void setRightVisible(boolean visible) {
        prefs.putBoolean(KEY_RIGHT_VISIBLE, visible);
    }

    public boolean isBottomVisible(boolean defaultValue) {
        return prefs.getBoolean(KEY_BOTTOM_VISIBLE, defaultValue);
    }

    public void setBottomVisible(boolean visible) {
        prefs.putBoolean(KEY_BOTTOM_VISIBLE, visible);
    }

    public DockPosition getLogDockPosition(DockPosition defaultPosition) {
        String value = prefs.get(KEY_LOG_DOCK, defaultPosition.name());
        try {
            return DockPosition.valueOf(value);
        } catch (IllegalArgumentException ex) {
            return defaultPosition;
        }
    }

    public void setLogDockPosition(DockPosition position) {
        prefs.put(KEY_LOG_DOCK, position.name());
    }

    public Rectangle loadWindowBounds(Rectangle defaultBounds) {
        int w = prefs.getInt(KEY_WINDOW_W, -1);
        int h = prefs.getInt(KEY_WINDOW_H, -1);
        int x = prefs.getInt(KEY_WINDOW_X, -1);
        int y = prefs.getInt(KEY_WINDOW_Y, -1);
        if (w > 0 && h > 0 && x >= 0 && y >= 0) {
            return new Rectangle(x, y, w, h);
        }
        return defaultBounds;
    }

    public void saveWindowBounds(Rectangle bounds) {
        if (bounds == null) return;
        prefs.putInt(KEY_WINDOW_W, bounds.width);
        prefs.putInt(KEY_WINDOW_H, bounds.height);
        prefs.putInt(KEY_WINDOW_X, bounds.x);
        prefs.putInt(KEY_WINDOW_Y, bounds.y);
    }

    public Rectangle loadLogFloatingBounds() {
        String raw = prefs.get(KEY_LOG_FLOAT_BOUNDS, null);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String[] parts = raw.split(",");
        if (parts.length != 4) return null;
        try {
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);
            int w = Integer.parseInt(parts[2]);
            int h = Integer.parseInt(parts[3]);
            return new Rectangle(x, y, w, h);
        } catch (Exception ignored) {
            return null;
        }
    }

    public void saveLogFloatingBounds(Rectangle bounds) {
        if (bounds == null) return;
        prefs.put(KEY_LOG_FLOAT_BOUNDS, bounds.x + "," + bounds.y + "," + bounds.width + "," + bounds.height);
    }

    public boolean isEditorMaximized(boolean defaultValue) {
        return prefs.getBoolean(KEY_EDITOR_MAX, defaultValue);
    }

    public void setEditorMaximized(boolean maximized) {
        prefs.putBoolean(KEY_EDITOR_MAX, maximized);
    }

    public void reset() {
        prefs.remove(KEY_LEFT_DIVIDER);
        prefs.remove(KEY_RIGHT_DIVIDER);
        prefs.remove(KEY_BOTTOM_DIVIDER);
        prefs.remove(KEY_LEFT_VISIBLE);
        prefs.remove(KEY_RIGHT_VISIBLE);
        prefs.remove(KEY_BOTTOM_VISIBLE);
        prefs.remove(KEY_LOG_DOCK);
        prefs.remove(KEY_WINDOW_X);
        prefs.remove(KEY_WINDOW_Y);
        prefs.remove(KEY_WINDOW_W);
        prefs.remove(KEY_WINDOW_H);
        prefs.remove(KEY_LOG_FLOAT_BOUNDS);
        prefs.remove(KEY_EDITOR_MAX);
    }
}
