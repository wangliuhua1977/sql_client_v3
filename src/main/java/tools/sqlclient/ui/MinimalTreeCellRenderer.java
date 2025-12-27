package tools.sqlclient.ui;

import tools.sqlclient.metadata.MetadataService;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.*;

/**
 * 轻量级树节点渲染，使用柔和的选中高亮与层级化的文字权重。
 */
public class MinimalTreeCellRenderer extends DefaultTreeCellRenderer {
    private final Font regularFont;
    private final Font boldFont;
    private final Color selectionBg = new Color(228, 236, 244);
    private final Color selectionBorder = new Color(200, 210, 220);
    private final Color schemaColor = new Color(84, 104, 130);
    private final Color objectColor = new Color(50, 120, 90);

    public MinimalTreeCellRenderer(Font baseFont) {
        Font font = baseFont != null ? baseFont : UIManager.getFont("Tree.font");
        this.regularFont = font != null ? font.deriveFont(Font.PLAIN) : null;
        this.boldFont = font != null ? font.deriveFont(Font.BOLD) : null;
        setBackgroundSelectionColor(selectionBg);
        setBorderSelectionColor(selectionBorder);
        setTextSelectionColor(new Color(25, 30, 40));
    }

    @Override
    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row,
                                                  boolean hasFocus) {
        JLabel label = (JLabel) super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
        if (regularFont != null) {
            label.setFont(regularFont);
        }
        if (value instanceof DefaultMutableTreeNode node) {
            Object user = node.getUserObject();
            if (user instanceof MetadataService.TableEntry entry) {
                String type = entry.type() == null ? "" : entry.type();
                label.setText(type.isBlank() ? entry.name() : entry.name() + " (" + type + ")");
                label.setForeground(objectColor);
                if (boldFont != null) {
                    label.setFont(boldFont);
                }
            } else if (node.getLevel() <= 1 && boldFont != null) {
                label.setFont(boldFont);
                label.setForeground(schemaColor);
            }
        }
        return label;
    }
}
