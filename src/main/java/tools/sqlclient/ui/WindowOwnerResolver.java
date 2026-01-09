package tools.sqlclient.ui;

import javax.swing.SwingUtilities;
import java.awt.Component;
import java.awt.Window;

/**
 * 解析 Swing 组件所属的顶层窗口，优先使用事件源组件的 ancestor，
 * 无法解析时回退到指定的默认窗口。
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
            owner = fallback;
        }
        return resolveRootOwner(owner);
    }

    private static Window resolveRootOwner(Window window) {
        if (window == null) {
            return null;
        }
        Window current = window;
        while (current.getOwner() != null) {
            current = current.getOwner();
        }
        return current;
    }
}
