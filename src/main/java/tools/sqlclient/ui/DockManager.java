package tools.sqlclient.ui;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 轻量停靠管理器，负责在底部、右侧以及浮动窗口之间移动组件。
 */
public class DockManager {
    private final JFrame owner;
    private final JTabbedPane bottomTabs;
    private final JTabbedPane rightTabs;
    private final Map<Component, DockPosition> positions = new HashMap<>();
    private final Map<Component, JDialog> floatingDialogs = new HashMap<>();

    public DockManager(JFrame owner, JTabbedPane bottomTabs, JTabbedPane rightTabs) {
        this.owner = owner;
        this.bottomTabs = bottomTabs;
        this.rightTabs = rightTabs;
    }

    public void dock(Component comp, DockPosition position, String title, Rectangle floatBounds) {
        SwingUtilities.invokeLater(() -> {
            removeFromParent(comp);
            if (position == DockPosition.BOTTOM) {
                ensureTab(bottomTabs, comp, title);
                bottomTabs.setSelectedComponent(comp);
            } else if (position == DockPosition.RIGHT) {
                ensureTab(rightTabs, comp, title);
                rightTabs.setSelectedComponent(comp);
            } else {
                JDialog dialog = floatingDialogs.computeIfAbsent(comp, c -> createDialog(title));
                dialog.setTitle(title);
                dialog.getContentPane().add(comp, BorderLayout.CENTER);
                if (floatBounds != null) {
                    dialog.setBounds(floatBounds);
                } else if (dialog.getWidth() == 0) {
                    dialog.setSize(480, 360);
                    dialog.setLocationRelativeTo(owner);
                }
                dialog.setVisible(true);
            }
            positions.put(comp, position);
            refreshContainers();
        });
    }

    public void restoreDock(Component comp, String title, Rectangle floatBounds) {
        DockPosition pos = positions.getOrDefault(comp, DockPosition.BOTTOM);
        dock(comp, pos, title, floatBounds);
    }

    public void undockToDialog(Component comp, String title, Rectangle floatBounds) {
        dock(comp, DockPosition.FLOATING, title, floatBounds);
    }

    public DockPosition getCurrentPosition(Component comp) {
        return positions.getOrDefault(comp, DockPosition.BOTTOM);
    }

    public Rectangle captureFloatingBounds(Component comp) {
        JDialog dialog = floatingDialogs.get(comp);
        if (dialog != null && dialog.isVisible()) {
            return dialog.getBounds();
        }
        return null;
    }

    public void hideFloating(Component comp) {
        JDialog dialog = floatingDialogs.get(comp);
        if (dialog != null) {
            dialog.setVisible(false);
        }
    }

    private void refreshContainers() {
        bottomTabs.revalidate();
        bottomTabs.repaint();
        rightTabs.revalidate();
        rightTabs.repaint();
    }

    private void ensureTab(JTabbedPane tabs, Component comp, String title) {
        int idx = tabs.indexOfComponent(comp);
        if (idx < 0) {
            tabs.addTab(title, comp);
        }
    }

    private JDialog createDialog(String title) {
        JDialog dialog = new JDialog(owner);
        dialog.setTitle(title);
        dialog.setLayout(new BorderLayout());
        dialog.setModal(false);
        dialog.setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        return dialog;
    }

    private void removeFromParent(Component comp) {
        Container parent = comp.getParent();
        if (parent == null) return;
        if (parent instanceof JTabbedPane tabs) {
            int idx = tabs.indexOfComponent(comp);
            if (idx >= 0) {
                tabs.remove(idx);
            }
        } else {
            parent.remove(comp);
        }
        parent.revalidate();
        parent.repaint();
    }
}
