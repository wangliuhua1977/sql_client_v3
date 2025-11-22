package tools.sqlclient.ui;

import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.nio.file.Path;

/**
 * 主窗口，符合 Windows 11 扁平化风格，全部中文。
 */
public class MainFrame extends JFrame {
    private final JTabbedPane tabbedPane = new JTabbedPane();
    private final JTree objectTree = new JTree();
    private final JLabel statusLabel = new JLabel("就绪");
    private final JLabel autosaveLabel = new JLabel("自动保存: -");
    private final JLabel taskLabel = new JLabel("后台任务: 0");
    private final MetadataService metadataService;

    public MainFrame() {
        super("SQL Notebook - 多标签 PG/Hive");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());

        this.metadataService = new MetadataService(Path.of("metadata.db"));
        buildMenu();
        buildToolbar();
        buildContent();
        buildStatusBar();
        metadataService.refreshMetadataAsync(this::reloadTree);
    }

    private void buildMenu() {
        JMenuBar menuBar = new JMenuBar();
        JMenu file = new JMenu("文件");
        JMenu edit = new JMenu("编辑");
        JMenu view = new JMenu("视图");
        JMenu tools = new JMenu("工具");
        JMenu help = new JMenu("帮助");

        file.add(new JMenuItem(new AbstractAction("新建标签") {
            @Override
            public void actionPerformed(ActionEvent e) {
                addTab(DatabaseType.POSTGRESQL);
            }
        }));
        file.add(new JMenuItem(new AbstractAction("保存全部") {
            @Override
            public void actionPerformed(ActionEvent e) {
                saveAll();
            }
        }));
        file.addSeparator();
        file.add(new JMenuItem(new AbstractAction("退出") {
            @Override
            public void actionPerformed(ActionEvent e) {
                dispose();
                System.exit(0);
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("刷新元数据") {
            @Override
            public void actionPerformed(ActionEvent e) {
                metadataService.refreshMetadataAsync(MainFrame.this::reloadTree);
            }
        }));

        menuBar.add(file);
        menuBar.add(edit);
        menuBar.add(view);
        menuBar.add(tools);
        menuBar.add(help);
        setJMenuBar(menuBar);
    }

    private void buildToolbar() {
        JToolBar bar = new JToolBar();
        bar.setFloatable(false);
        bar.add(new JButton(new AbstractAction("新建") {
            @Override
            public void actionPerformed(ActionEvent e) {
                addTab(DatabaseType.POSTGRESQL);
            }
        }));
        bar.add(new JButton(new AbstractAction("打开") {
            @Override
            public void actionPerformed(ActionEvent e) {
                // 简化：示例中未实现打开逻辑
                JOptionPane.showMessageDialog(MainFrame.this, "示例中仅演示新建/自动保存");
            }
        }));
        bar.add(new JButton(new AbstractAction("保存") {
            @Override
            public void actionPerformed(ActionEvent e) {
                EditorTabPanel panel = getCurrentTab();
                if (panel != null) panel.saveNow();
            }
        }));
        bar.add(new JButton(new AbstractAction("保存全部") {
            @Override
            public void actionPerformed(ActionEvent e) {
                saveAll();
            }
        }));
        bar.addSeparator();
        bar.add(new JButton(new AbstractAction("刷新元数据") {
            @Override
            public void actionPerformed(ActionEvent e) {
                metadataService.refreshMetadataAsync(MainFrame.this::reloadTree);
            }
        }));

        add(bar, BorderLayout.NORTH);
    }

    private void buildContent() {
        JSplitPane splitPane = new JSplitPane();
        splitPane.setResizeWeight(0.2);
        JScrollPane treeScroll = new JScrollPane(objectTree);
        treeScroll.setBorder(new EmptyBorder(5,5,5,5));
        splitPane.setLeftComponent(treeScroll);
        splitPane.setRightComponent(tabbedPane);
        add(splitPane, BorderLayout.CENTER);
        addTab(DatabaseType.POSTGRESQL);
    }

    private void buildStatusBar() {
        JPanel status = new JPanel(new BorderLayout());
        status.setBorder(new EmptyBorder(4,8,4,8));
        JPanel left = new JPanel(new FlowLayout(FlowLayout.LEFT, 10,0));
        left.add(statusLabel);
        left.add(autosaveLabel);
        left.add(taskLabel);
        status.add(left, BorderLayout.WEST);
        add(status, BorderLayout.SOUTH);
    }

    private void addTab(DatabaseType type) {
        EditorTabPanel panel = new EditorTabPanel(type, metadataService, this::updateAutosaveTime, this::updateTaskCount);
        tabbedPane.addTab(type == DatabaseType.POSTGRESQL ? "PG 笔记" : "Hive 笔记", panel);
        tabbedPane.setSelectedComponent(panel);
    }

    private EditorTabPanel getCurrentTab() {
        Component comp = tabbedPane.getSelectedComponent();
        if (comp instanceof EditorTabPanel panel) return panel;
        return null;
    }

    private void saveAll() {
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            if (comp instanceof EditorTabPanel panel) {
                panel.saveNow();
            }
        }
    }

    private void reloadTree() {
        DefaultMutableTreeNode root = new DefaultMutableTreeNode("对象浏览器");
        metadataService.loadTree(root);
        objectTree.setModel(new DefaultTreeModel(root));
    }

    private void updateAutosaveTime(String text) {
        SwingUtilities.invokeLater(() -> autosaveLabel.setText("自动保存: " + text));
    }

    private void updateTaskCount(int count) {
        SwingUtilities.invokeLater(() -> taskLabel.setText("后台任务: " + count));
    }
}
