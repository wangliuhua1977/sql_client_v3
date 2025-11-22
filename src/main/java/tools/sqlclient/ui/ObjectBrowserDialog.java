package tools.sqlclient.ui;

import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.metadata.MetadataService.TableWithColumns;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
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

        add(keyword, BorderLayout.NORTH);
        add(new JScrollPane(tree), BorderLayout.CENTER);

        refresh("");
    }

    private void refresh(String keyword) {
        SwingUtilities.invokeLater(() -> {
            root.removeAllChildren();
            List<TableWithColumns> tables = metadataService.listTablesWithColumns(keyword);
            for (TableWithColumns table : tables) {
                DefaultMutableTreeNode tableNode = new DefaultMutableTreeNode(table.name() + " (" + table.type() + ")");
                for (String col : table.columns()) {
                    tableNode.add(new DefaultMutableTreeNode(col));
                }
                root.add(tableNode);
            }
            DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
            model.reload();
            for (int i = 0; i < tree.getRowCount(); i++) {
                tree.expandRow(i);
            }
        });
    }
}

