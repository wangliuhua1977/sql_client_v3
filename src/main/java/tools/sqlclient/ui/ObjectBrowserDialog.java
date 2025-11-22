package tools.sqlclient.ui;

import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.metadata.MetadataService.TableEntry;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.*;
import java.util.List;

/**
 * 简易对象浏览器，可按表名模糊搜索，展示表/视图与其字段。由菜单触发。
 */
public class ObjectBrowserDialog extends JDialog {
    private final MetadataService metadataService;
    private final DefaultMutableTreeNode root = new DefaultMutableTreeNode("对象");
    private final JTree tree = new JTree(root);

    public ObjectBrowserDialog(JFrame owner, MetadataService metadataService) {
        super(owner, "对象浏览器", false);
        this.metadataService = metadataService;
        setSize(420, 600);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout());

        JTextField keyword = new JTextField();
        keyword.setToolTipText("输入表名关键字，支持%分隔模糊");
        keyword.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) { refresh(keyword.getText()); }

            @Override
            public void removeUpdate(DocumentEvent e) { refresh(keyword.getText()); }

            @Override
            public void changedUpdate(DocumentEvent e) { refresh(keyword.getText()); }
        });

        tree.setRootVisible(true);
        tree.setShowsRootHandles(true);
        tree.setCellRenderer(new DefaultTreeCellRenderer() {
            @Override
            public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
                JLabel label = (JLabel) super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
                if (value instanceof DefaultMutableTreeNode node) {
                    Object user = node.getUserObject();
                    if (user instanceof TableEntry entry) {
                        label.setText(entry.name() + " (" + entry.type() + ")");
                    }
                }
                return label;
            }
        });
        tree.addTreeSelectionListener(e -> {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();
            if (node == null) return;
            Object user = node.getUserObject();
            if (user instanceof TableEntry entry) {
                loadColumnsLazy(node, entry.name());
            }
        });

        add(keyword, BorderLayout.NORTH);
        add(new JScrollPane(tree), BorderLayout.CENTER);

        refresh("");
    }

    private void refresh(String keyword) {
        SwingUtilities.invokeLater(() -> {
            root.removeAllChildren();
            List<TableEntry> tables = metadataService.listTables(keyword);
            for (TableEntry table : tables) {
                DefaultMutableTreeNode tableNode = new DefaultMutableTreeNode(table);
                root.add(tableNode);
            }
            DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
            model.reload();
        });
    }

    private void loadColumnsLazy(DefaultMutableTreeNode tableNode, String tableName) {
        if (tableNode.getChildCount() > 0) return; // 已加载
        // 本地缓存直接渲染
        List<String> cached = metadataService.loadColumnsFromCache(tableName);
        if (!cached.isEmpty()) {
            cached.forEach(col -> tableNode.add(new DefaultMutableTreeNode(col)));
            ((DefaultTreeModel) tree.getModel()).reload(tableNode);
            tree.expandPath(new TreePath(tableNode.getPath()));
            return;
        }
        // 远程补齐再刷新
        metadataService.ensureColumnsCachedAsync(tableName, () -> {
            List<String> cols = metadataService.loadColumnsFromCache(tableName);
            SwingUtilities.invokeLater(() -> {
                for (String col : cols) {
                    tableNode.add(new DefaultMutableTreeNode(col));
                }
                ((DefaultTreeModel) tree.getModel()).reload(tableNode);
                tree.expandPath(new TreePath(tableNode.getPath()));
            });
        });
    }
}

