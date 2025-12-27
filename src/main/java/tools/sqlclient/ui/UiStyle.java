package tools.sqlclient.ui;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.border.MatteBorder;
import javax.swing.plaf.FontUIResource;
import java.awt.*;
import java.util.Enumeration;

/**
 * 统一的 UI 轻量化样式：字体、行高、留白与分割线。
 */
public final class UiStyle {
    public static final Color BORDER = new Color(225, 229, 235);
    public static final Color SECTION_BG = new Color(246, 248, 251);
    public static final Color ACCENT = new Color(64, 132, 214);
    public static final Color SELECTION = new Color(223, 234, 249);

    private UiStyle() {
    }

    public static void installGlobalDefaults() {
        Font baseFont = new Font("SansSerif", Font.PLAIN, 12);
        FontUIResource resource = new FontUIResource(baseFont);
        Enumeration<Object> keys = UIManager.getDefaults().keys();
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            Object value = UIManager.get(key);
            if (value instanceof FontUIResource) {
                UIManager.put(key, resource);
            }
        }
        UIManager.put("defaultFont", resource);
        UIManager.put("Table.rowHeight", 24);
        UIManager.put("Tree.rowHeight", 22);
        UIManager.put("TabbedPane.contentAreaColor", Color.WHITE);
        UIManager.put("Component.focusWidth", 1);
    }

    public static Border panelPadding() {
        return new EmptyBorder(12, 12, 12, 12);
    }

    public static Border sectionLine() {
        return new MatteBorder(0, 0, 1, 0, BORDER);
    }

    public static Border thinLine() {
        return new MatteBorder(1, 1, 1, 1, BORDER);
    }
}
