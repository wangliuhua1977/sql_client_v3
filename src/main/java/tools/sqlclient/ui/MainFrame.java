package tools.sqlclient.ui;

import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.nio.file.Path;

/**
 * 主窗口，符合 Windows 11 扁平化风格，全部中文。
 */
public class MainFrame extends JFrame {
    private final JTabbedPane tabbedPane = new JTabbedPane();
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
        buildContent();
        buildStatusBar();
        metadataService.refreshMetadataAsync(() -> {});
    }

    private void buildMenu() {
        JMenuBar menuBar = new JMenuBar();
        JMenu file = new JMenu("文件");
        JMenu edit = new JMenu("编辑");
        JMenu view = new JMenu("视图");
        JMenu tools = new JMenu("工具");
        JMenu help = new JMenu("帮助");

        file.add(new JMenuItem(new AbstractAction("新建 PostgreSQL 标签") {
            @Override
            public void actionPerformed(ActionEvent e) {
                addTab(DatabaseType.POSTGRESQL);
            }
        }));
        file.add(new JMenuItem(new AbstractAction("新建 Hive 标签") {
            @Override
            public void actionPerformed(ActionEvent e) {
                addTab(DatabaseType.HIVE);
            }
        }));
        file.addSeparator();
        file.add(new JMenuItem(new AbstractAction("打开文件") {
            @Override
            public void actionPerformed(ActionEvent e) {
                JOptionPane.showMessageDialog(MainFrame.this, "示例版本未实现打开文件，可直接新建标签编辑");
            }
        }));
        file.add(new JMenuItem(new AbstractAction("保存当前") {
            @Override
            public void actionPerformed(ActionEvent e) {
                EditorTabPanel panel = getCurrentTab();
                if (panel != null) panel.saveNow();
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
                metadataService.refreshMetadataAsync(() -> {});
            }
        }));

        menuBar.add(file);
        menuBar.add(edit);
        menuBar.add(view);
        menuBar.add(tools);
        menuBar.add(help);
        setJMenuBar(menuBar);
    }

    private void buildContent() {
        add(tabbedPane, BorderLayout.CENTER);
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

    private void updateAutosaveTime(String text) {
        SwingUtilities.invokeLater(() -> autosaveLabel.setText("自动保存: " + text));
    }

    private void updateTaskCount(int count) {
        SwingUtilities.invokeLater(() -> taskLabel.setText("后台任务: " + count));
    }
}
