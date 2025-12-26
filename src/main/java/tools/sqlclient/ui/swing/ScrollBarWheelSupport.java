package tools.sqlclient.ui.swing;

import javax.swing.*;
import java.awt.event.MouseWheelListener;

/**
 * 为垂直滚动条添加鼠标滚轮支持，确保在滚动条区域滚动时内容也会滚动。
 */
public final class ScrollBarWheelSupport {
    private static final String INSTALLED_KEY = "wheelSupportInstalled";

    private ScrollBarWheelSupport() {
    }

    public static void enableWheelOnVerticalScrollBar(JScrollPane scrollPane) {
        if (scrollPane == null) {
            return;
        }
        if (SwingUtilities.isEventDispatchThread()) {
            install(scrollPane);
        } else {
            SwingUtilities.invokeLater(() -> install(scrollPane));
        }
    }

    private static void install(JScrollPane scrollPane) {
        if (scrollPane == null) {
            return;
        }
        JScrollBar vbar = scrollPane.getVerticalScrollBar();
        if (vbar == null) {
            return;
        }
        Object installed = vbar.getClientProperty(INSTALLED_KEY);
        if (installed instanceof Boolean && (Boolean) installed) {
            return;
        }
        vbar.putClientProperty(INSTALLED_KEY, Boolean.TRUE);
        MouseWheelListener listener = e -> {
            int units = e.getUnitsToScroll();
            int inc = Math.max(1, vbar.getUnitIncrement(units));
            if (inc <= 0) {
                inc = Math.max(1, vbar.getBlockIncrement(units));
            }
            int delta = units * inc;
            int newValue = clamp(vbar.getValue() + delta, vbar.getMinimum(), vbar.getMaximum());
            vbar.setValue(newValue);
            e.consume();
        };
        vbar.addMouseWheelListener(listener);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }
}
