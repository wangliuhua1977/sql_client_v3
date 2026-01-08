package tools.sqlclient.ui.scroll;

import javax.swing.*;
import javax.swing.text.JTextComponent;
import java.awt.*;
import java.awt.event.MouseWheelListener;
import java.lang.ref.WeakReference;

/**
 * Adds wheel scrolling support on the editor area itself while keeping the actual scroll
 * adjustment on the vertical scroll bar. This ensures consistent behavior when the mouse is
 * hovering anywhere inside the focused SQL editor.
 */
public final class EditorWheelScrollSupport {

    private static final String INSTALLED_KEY = "editorWheelScrollSupport.installed";

    private EditorWheelScrollSupport() {
    }

    /**
     * Installs the wheel listener on the given editor to forward wheel motion to the provided
     * scroll pane's vertical scroll bar.
     */
    public static void install(JTextComponent editor, JScrollPane scrollPane) {
        if (editor == null || scrollPane == null) {
            return;
        }
        if (SwingUtilities.isEventDispatchThread()) {
            doInstall(editor, scrollPane);
        } else {
            SwingUtilities.invokeLater(() -> doInstall(editor, scrollPane));
        }
    }

    private static void doInstall(JTextComponent editor, JScrollPane scrollPane) {
        Object installed = editor.getClientProperty(INSTALLED_KEY);
        if (installed instanceof Boolean && (Boolean) installed) {
            return;
        }
        editor.putClientProperty(INSTALLED_KEY, Boolean.TRUE);

        WeakReference<JScrollPane> scrollRef = new WeakReference<>(scrollPane);
        MouseWheelListener listener = e -> {
            if (e.isShiftDown()) {
                return;
            }

            Component source = e.getComponent();
            JScrollPane sp = null;
            if (source != null) {
                Component ancestor = SwingUtilities.getAncestorOfClass(JScrollPane.class, source);
                if (ancestor instanceof JScrollPane pane) {
                    sp = pane;
                }
            }
            if (sp == null) {
                sp = scrollRef.get();
            }
            if (sp == null) {
                return;
            }
            JScrollBar vbar = sp.getVerticalScrollBar();
            if (vbar == null) {
                return;
            }

            int units = e.getUnitsToScroll();
            int inc = vbar.getUnitIncrement(units);
            if (inc == 0) {
                inc = vbar.getUnitIncrement(units > 0 ? 1 : -1);
            }
            if (inc == 0) {
                inc = vbar.getBlockIncrement(units > 0 ? 1 : -1);
            }
            if (inc == 0) {
                inc = 1;
            }

            int delta = units * inc;
            // Use (maximum - visibleAmount) as the upper bound to avoid overscrolling past content end
            int maxValue = Math.max(vbar.getMinimum(), vbar.getMaximum() - vbar.getVisibleAmount());
            int newValue = clamp(vbar.getValue() + delta, vbar.getMinimum(), maxValue);
            vbar.setValue(newValue);
            e.consume();
        };
        editor.addMouseWheelListener(listener);
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
