package tools.sqlclient.ui;

import tools.sqlclient.metadata.MetadataService;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.*;

/**
 * 极简风格的树节点渲染器，区分表/视图/例程并减弱边框噪声。
 */
public class MinimalTreeCellRenderer extends DefaultTreeCellRenderer {
    private final Color selectionBg = new Color(0xE6ECF5);
    private final Color selectionFg = new Color(0x102A43);
    private final Color tableColor = new Color(0x173D6A);
    private final Color viewColor = new Color(0x2D6AA3);
    private final Color routineColor = new Color(0x2F855A);
    private final Color groupColor = new Color(0x52616B);
    private final Font plainFont;
    private final Font boldFont;

    public MinimalTreeCellRenderer(Font baseFont) {
        Font base = baseFont != null ? baseFont : UIManager.getFont("Tree.font");
        this.plainFont = base;
        this.boldFont = base.deriveFont(Font.BOLD);
        setOpenIcon(null);
        setClosedIcon(null);
        setLeafIcon(null);
    }

    @Override
    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row,
                                                  boolean hasFocus) {
        JLabel label = (JLabel) super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
        label.setBorder(new EmptyBorder(2, 8, 2, 4));
        label.setIcon(null);
        label.setOpaque(true);
        label.setBackground(sel ? selectionBg : Color.WHITE);
        label.setForeground(sel ? selectionFg : new Color(0x1F2933));
        label.setFont(plainFont);

        if (value instanceof DefaultMutableTreeNode node) {
            Object user = node.getUserObject();
            if (user instanceof MetadataService.TableEntry entry) {
                label.setText(entry.name());
                String type = entry.type() == null ? "" : entry.type().toLowerCase();
                switch (type) {
                    case "table" -> {
                        label.setFont(boldFont);
                        label.setForeground(sel ? selectionFg : tableColor);
                    }
                    case "view" -> {
                        label.setFont(boldFont);
                        label.setForeground(sel ? selectionFg : viewColor);
                    }
                    case "function", "procedure" -> label.setForeground(sel ? selectionFg : routineColor);
                    default -> label.setForeground(sel ? selectionFg : new Color(0x314254));
                }
            } else if (node.getParent() == null || node.getParent() == tree.getModel().getRoot()) {
                label.setFont(boldFont);
                label.setForeground(sel ? selectionFg : groupColor);
            }
        }
        return label;
    }
}
