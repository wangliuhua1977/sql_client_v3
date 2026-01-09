package tools.sqlclient.ui;

import javax.swing.SwingUtilities;
import java.awt.Component;
import java.awt.Window;

/**
 * 解析 Swing 组件所属的顶层窗口，优先使用事件源组件的 ancestor，
 * 其次使用当前激活窗口，再回退到指定的默认窗口。
 */
public final class WindowOwnerResolver {
    private WindowOwnerResolver() {
    }

    public static Window resolve(Component source, Window fallback) {
        Window owner = null;
        if (source != null) {
            owner = SwingUtilities.getWindowAncestor(source);
        }
        if (owner == null) {
            owner = activeWindow();
        }
        if (owner == null) {
            owner = fallback;
        }
        return owner;
    }

    private static Window activeWindow() {
        for (Window window : Window.getWindows()) {
            if (window.isActive()) {
                return window;
            }
        }
        return null;
    }
}
