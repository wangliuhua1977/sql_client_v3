package tools.sqlclient.ui;

import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.metadata.MetadataService.TableEntry;
import tools.sqlclient.pg.PgRoutineService;
import tools.sqlclient.pg.RoutineInfo;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 简易对象浏览器，可按表名模糊搜索，展示表/视图/函数/过程与其字段。由菜单触发。
 */
public class ObjectBrowserDialog extends JDialog {
    private final MetadataService metadataService;
    private final PgRoutineService routineService;
    private final RoutineActionHandler routineHandler;
    private final DefaultMutableTreeNode root = new DefaultMutableTreeNode("对象");
    private final DefaultMutableTreeNode tablesRoot = new DefaultMutableTreeNode("表/视图");
    private final DefaultMutableTreeNode functionsRoot = new DefaultMutableTreeNode("函数");
    private final DefaultMutableTreeNode proceduresRoot = new DefaultMutableTreeNode("存储过程");
    private final JTree tree = new JTree(root);
    private final JPopupMenu popup = new JPopupMenu();
    private final JMenuItem refreshColumnsItem = new JMenuItem("刷新字段（远程）");
    private final JMenuItem openSourceItem = new JMenuItem("打开源码（只读）");
    private final JMenuItem editSourceItem = new JMenuItem("编辑源码");
    private final JMenuItem runRoutineItem = new JMenuItem("运行/调试运行…");

    public interface RoutineActionHandler {
        void openRoutine(RoutineInfo info, boolean editable);

        void runRoutine(RoutineInfo info);
    }

    public ObjectBrowserDialog(JFrame owner, MetadataService metadataService,
                               PgRoutineService routineService,
                               RoutineActionHandler routineHandler) {
        super(owner, "对象浏览器", false);
        this.metadataService = metadataService;
        this.routineService = routineService;
        this.routineHandler = routineHandler;
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
            if (user instanceof TableEntry entry && isTableLike(entry)) {
                loadColumnsLazy(node, entry.name(), false);
            }
            updateMenuVisibility();
        });

        refreshColumnsItem.addActionListener(e -> {
            TreePath path = tree.getSelectionPath();
            if (path == null) return;
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
            Object user = node.getUserObject();
            if (user instanceof TableEntry entry && isTableLike(entry)) {
                loadColumnsLazy(node, entry.name(), true);
            }
        });
        openSourceItem.addActionListener(e -> handleRoutineAction(false, false));
        editSourceItem.addActionListener(e -> handleRoutineAction(true, false));
        runRoutineItem.addActionListener(e -> handleRoutineAction(true, true));

        popup.add(refreshColumnsItem);
        popup.add(openSourceItem);
        popup.add(editSourceItem);
        popup.add(runRoutineItem);
        tree.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mousePressed(java.awt.event.MouseEvent e) { handlePopup(e); }

            @Override
            public void mouseReleased(java.awt.event.MouseEvent e) { handlePopup(e); }

            private void handlePopup(java.awt.event.MouseEvent e) {
                if (e.isPopupTrigger()) {
                    int row = tree.getRowForLocation(e.getX(), e.getY());
                    if (row >= 0) {
                        tree.setSelectionRow(row);
                        updateMenuVisibility();
                        popup.show(tree, e.getX(), e.getY());
                    }
                }
            }
        });

        add(keyword, BorderLayout.NORTH);
        add(new JScrollPane(tree), BorderLayout.CENTER);

        refresh("");
    }

    public void reload() {
        refresh("");
    }

    private void refresh(String keyword) {
        SwingUtilities.invokeLater(() -> {
            root.removeAllChildren();
            tablesRoot.removeAllChildren();
            functionsRoot.removeAllChildren();
            proceduresRoot.removeAllChildren();
            root.add(tablesRoot);
            root.add(functionsRoot);
            root.add(proceduresRoot);

            List<TableEntry> tables = metadataService.listTables(keyword);
            for (TableEntry table : tables) {
                DefaultMutableTreeNode tableNode = new DefaultMutableTreeNode(table);
                tablesRoot.add(tableNode);
            }

            metadataService.listRoutines(keyword, "function")
                    .forEach(func -> functionsRoot.add(new DefaultMutableTreeNode(func)));
            metadataService.listRoutines(keyword, "procedure")
                    .forEach(proc -> proceduresRoot.add(new DefaultMutableTreeNode(proc)));
            DefaultTreeModel model = (DefaultTreeModel) tree.getModel();
            model.reload();
        });
    }

    private boolean isTableLike(TableEntry entry) {
        String type = entry.type() == null ? "" : entry.type().toLowerCase();
        return "table".equals(type) || "view".equals(type);
    }

    private boolean isRoutine(TableEntry entry) {
        String type = entry.type() == null ? "" : entry.type().toLowerCase();
        return "function".equals(type) || "procedure".equals(type);
    }

    private void updateMenuVisibility() {
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) tree.getLastSelectedPathComponent();
        boolean table = false;
        boolean routine = false;
        if (node != null && node.getUserObject() instanceof TableEntry entry) {
            table = isTableLike(entry);
            routine = isRoutine(entry);
        }
        refreshColumnsItem.setVisible(table);
        openSourceItem.setVisible(routine);
        editSourceItem.setVisible(routine);
        runRoutineItem.setVisible(routine);
    }

    private void handleRoutineAction(boolean editable, boolean run) {
        TreePath path = tree.getSelectionPath();
        if (path == null) return;
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
        Object user = node.getUserObject();
        if (!(user instanceof TableEntry entry) || routineService == null || routineHandler == null) {
            return;
        }
        if (!isRoutine(entry)) {
            return;
        }
        routineService.listLeshanRoutinesByName(entry.name()).whenComplete((list, ex) ->
                SwingUtilities.invokeLater(() -> {
                    if (ex != null || list == null) {
                        JOptionPane.showMessageDialog(this, "获取重载列表失败: " + (ex != null ? ex.getMessage() : ""));
                        return;
                    }
                    List<RoutineInfo> filtered = new ArrayList<>();
                    for (RoutineInfo info : list) {
                        if (matchesType(entry.type(), info)) {
                            filtered.add(info);
                        }
                    }
                    if (filtered.isEmpty()) {
                        JOptionPane.showMessageDialog(this, "未找到匹配的重载");
                        return;
                    }
                    RoutineInfo target;
                    if (filtered.size() == 1) {
                        target = filtered.get(0);
                    } else {
                        RoutineOverloadPickerDialog dialog = new RoutineOverloadPickerDialog((Frame) getOwner(), filtered);
                        dialog.setVisible(true);
                        target = dialog.getSelected();
                        if (target == null) {
                            return;
                        }
                    }
                    if (run) {
                        routineHandler.runRoutine(target);
                    } else {
                        routineHandler.openRoutine(target, editable);
                    }
                })
        );
    }

    private boolean matchesType(String type, RoutineInfo info) {
        String lower = type == null ? "" : type.toLowerCase();
        if ("procedure".equals(lower)) {
            return info.isProcedure();
        }
        if ("function".equals(lower)) {
            return info.isFunction();
        }
        return true;
    }

    private void loadColumnsLazy(DefaultMutableTreeNode tableNode, String tableName, boolean forceRefresh) {
        if (!forceRefresh && tableNode.getChildCount() > 0) return; // 已加载
        tableNode.removeAllChildren();
        // 本地缓存直接渲染
        List<String> cached = metadataService.loadColumnsFromCache(tableName);
        if (!cached.isEmpty() && !forceRefresh) {
            cached.forEach(col -> tableNode.add(new DefaultMutableTreeNode(col)));
            ((DefaultTreeModel) tree.getModel()).reload(tableNode);
            tree.expandPath(new TreePath(tableNode.getPath()));
            return;
        }
        // 远程补齐再刷新
        metadataService.ensureColumnsCachedAsync(tableName, forceRefresh, () -> {
            List<String> cols = metadataService.loadColumnsFromCache(tableName);
            SwingUtilities.invokeLater(() -> {
                tableNode.removeAllChildren();
                for (String col : cols) {
                    tableNode.add(new DefaultMutableTreeNode(col));
                }
                ((DefaultTreeModel) tree.getModel()).reload(tableNode);
                tree.expandPath(new TreePath(tableNode.getPath()));
            });
        });
    }
}

