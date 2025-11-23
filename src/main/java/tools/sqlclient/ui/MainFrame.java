package tools.sqlclient.ui;

import tools.sqlclient.db.AppStateRepository;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.Note;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.plaf.basic.BasicInternalFrameUI;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主窗口，符合 Windows 11 扁平化风格，全部中文。
 */
public class MainFrame extends JFrame {
    private final CardLayout centerLayout = new CardLayout();
    private final JPanel centerPanel = new JPanel(centerLayout);
    private final JDesktopPane desktopPane = new JDesktopPane();
    private final JTabbedPane tabbedPane = new JTabbedPane();
    private final JLabel statusLabel = new JLabel("就绪");
    private final JLabel autosaveLabel = new JLabel("自动保存: -");
    private final JLabel taskLabel = new JLabel("后台任务: 0");
    private final SQLiteManager sqliteManager;
    private final NoteRepository noteRepository;
    private final AppStateRepository appStateRepository;
    private final MetadataService metadataService;
    private final AtomicInteger untitledIndex = new AtomicInteger(1);
    private final java.util.Map<Long, EditorTabPanel> panelCache = new java.util.HashMap<>();
    private boolean windowMode = true;
    private JRadioButtonMenuItem windowModeItem;
    private JRadioButtonMenuItem panelModeItem;
    private boolean convertFullWidth = true;

    public MainFrame() {
        super("SQL Notebook - 多标签 PG/Hive");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());

        this.sqliteManager = new SQLiteManager(java.nio.file.Path.of("metadata.db"));
        this.metadataService = new MetadataService(java.nio.file.Path.of("metadata.db"));
        this.noteRepository = new NoteRepository(sqliteManager);
        this.appStateRepository = new AppStateRepository(sqliteManager);
        this.convertFullWidth = appStateRepository.loadFullWidthOption(true);
        buildMenu();
        buildContent();
        buildStatusBar();
        metadataService.refreshMetadataAsync(() -> {});
        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent e) {
                persistOpenFrames();
            }
        });
    }

    private void buildMenu() {
        JMenuBar menuBar = new JMenuBar();
        JMenu file = new JMenu("文件");
        JMenu edit = new JMenu("编辑");
        JMenu view = new JMenu("视图");
        JMenu window = new JMenu("窗口");
        JMenu tools = new JMenu("工具");
        JMenu help = new JMenu("帮助");

        file.add(new JMenuItem(new AbstractAction("新建 PostgreSQL 笔记") {
            @Override
            public void actionPerformed(ActionEvent e) {
                createNote(DatabaseType.POSTGRESQL);
            }
        }));
        file.add(new JMenuItem(new AbstractAction("新建 Hive 笔记") {
            @Override
            public void actionPerformed(ActionEvent e) {
                createNote(DatabaseType.HIVE);
            }
        }));
        file.addSeparator();
        file.add(new JMenuItem(new AbstractAction("导入笔记") {
            @Override
            public void actionPerformed(ActionEvent e) {
                importNoteFromFile();
            }
        }));
        file.add(new JMenuItem(new AbstractAction("保存当前") {
            @Override
            public void actionPerformed(ActionEvent e) {
                EditorTabPanel panel = getCurrentPanel();
                if (panel != null) panel.saveNow();
            }
        }));
        file.add(new JMenuItem(new AbstractAction("保存全部") {
            @Override
            public void actionPerformed(ActionEvent e) {
                saveAll();
            }
        }));
        file.add(new JMenuItem(new AbstractAction("管理笔记") {
            @Override
            public void actionPerformed(ActionEvent e) {
                openManageDialog();
            }
        }));
        file.add(new JMenuItem(new AbstractAction("设置 - SQL 编辑器选项") {
            @Override
            public void actionPerformed(ActionEvent e) {
                openEditorSettings();
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

        view.add(new JMenuItem(new AbstractAction("对象浏览器") {
            @Override
            public void actionPerformed(ActionEvent e) {
                ObjectBrowserDialog dialog = new ObjectBrowserDialog(MainFrame.this, metadataService);
                dialog.setVisible(true);
            }
        }));

        view.addSeparator();
        windowModeItem = new JRadioButtonMenuItem("独立窗口模式", true);
        panelModeItem = new JRadioButtonMenuItem("面板模式", false);
        ButtonGroup group = new ButtonGroup();
        group.add(windowModeItem);
        group.add(panelModeItem);

        windowModeItem.addActionListener(e -> switchToWindowMode());
        panelModeItem.addActionListener(e -> switchToPanelMode());
        view.add(windowModeItem);
        view.add(panelModeItem);

        window.add(new JMenuItem(new AbstractAction("平铺窗口") {
            @Override
            public void actionPerformed(ActionEvent e) {
                tileFrames();
            }
        }));

        menuBar.add(file);
        menuBar.add(edit);
        menuBar.add(view);
        menuBar.add(window);
        menuBar.add(tools);
        menuBar.add(help);
        setJMenuBar(menuBar);
    }

    private void buildContent() {
        centerPanel.add(desktopPane, "window");
        centerPanel.add(tabbedPane, "panel");
        add(centerPanel, BorderLayout.CENTER);
        centerLayout.show(centerPanel, "window");
        restoreSession();
    }

    private void restoreSession() {
        java.util.List<Long> openIds = appStateRepository.loadOpenNotes();
        if (!openIds.isEmpty()) {
            noteRepository.listByIds(openIds).forEach(this::openNoteInCurrentMode);
        }
        if (panelCache.isEmpty()) {
            createNote(DatabaseType.POSTGRESQL);
        }
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

    private void createNote(DatabaseType type) {
        boolean created = false;
        while (!created) {
            String title = "未命名" + untitledIndex.getAndIncrement();
            try {
                Note note = noteRepository.create(title, type);
                openNoteInCurrentMode(note);
                created = true;
            } catch (IllegalArgumentException ex) {
                // 重复则继续尝试下一个序号
            }
        }
    }

    private EditorTabPanel getOrCreatePanel(Note note) {
        return panelCache.computeIfAbsent(note.getId(), id -> new EditorTabPanel(noteRepository, metadataService,
                this::updateAutosaveTime, this::updateTaskCount,
                newTitle -> updateTitleForPanel(newTitle, id), note, convertFullWidth));
    }

    private void openNoteInCurrentMode(Note note) {
        if (windowMode) {
            if (!focusExistingFrame(note.getId())) {
                addFrame(getOrCreatePanel(note));
            }
        } else {
            if (!focusExistingTab(note.getId())) {
                addTab(getOrCreatePanel(note));
            }
        }
    }

    private boolean focusExistingFrame(Long noteId) {
        long targetId = noteId != null ? noteId : -1L;
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel panel = extractPanelFromFrame(frame);
            if (panel != null && panel.getNote().getId() == targetId) {
                try {
                    frame.setIcon(false);
                    frame.setSelected(true);
                    frame.toFront();
                } catch (Exception ignored) {}
                return true;
            }
        }
        return false;
    }

    private boolean focusExistingTab(Long noteId) {
        long targetId = noteId != null ? noteId : -1L;
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            EditorTabPanel panel = extractPanel(comp);
            if (panel != null && panel.getNote().getId() == targetId) {
                tabbedPane.setSelectedIndex(i);
                return true;
            }
        }
        return false;
    }

    private void addFrame(EditorTabPanel panel) {
        detachFromParent(panel);
        JInternalFrame frame = new JInternalFrame(panel.getNote().getTitle(), true, true, true, true);
        frame.setSize(600, 400);
        frame.setLocation(20 * desktopPane.getAllFrames().length, 20 * desktopPane.getAllFrames().length);
        frame.setVisible(true);
        frame.add(panel, BorderLayout.CENTER);
        installRenameHandler(frame, panel);
        desktopPane.add(frame);
        try {
            frame.setSelected(true);
        } catch (java.beans.PropertyVetoException ignored) { }
        persistOpenFrames();
    }

    private void addTab(EditorTabPanel panel) {
        detachFromParent(panel);
        tabbedPane.addTab(panel.getNote().getTitle(), panel);
        int idx = tabbedPane.indexOfComponent(panel);
        tabbedPane.setSelectedIndex(idx);
        persistOpenFrames();
    }

    private void detachFromParent(EditorTabPanel panel) {
        Container parent = panel.getParent();
        if (parent != null) {
            parent.remove(panel);
            parent.revalidate();
            parent.repaint();
        }
    }

    private void installRenameHandler(JInternalFrame frame, EditorTabPanel panel) {
        BasicInternalFrameUI ui = (BasicInternalFrameUI) frame.getUI();
        if (ui != null && ui.getNorthPane() != null) {
            ui.getNorthPane().addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent e) {
                    if (e.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(e)) {
                        String newTitle = JOptionPane.showInputDialog(MainFrame.this, "重命名窗口", frame.getTitle());
                        if (newTitle != null && !newTitle.trim().isEmpty()) {
                            panel.rename(newTitle.trim());
                            frame.setTitle(newTitle.trim());
                            updateTabTitle(panel, newTitle.trim());
                        }
                        e.consume();
                    }
                }
            });
        }
    }

    private EditorTabPanel getCurrentPanel() {
        if (windowMode) {
            JInternalFrame frame = desktopPane.getSelectedFrame();
            if (frame != null && frame.getContentPane().getComponentCount() > 0) {
                return extractPanel(frame.getContentPane().getComponent(0));
            }
        } else {
            Component comp = tabbedPane.getSelectedComponent();
            return extractPanel(comp);
        }
        return null;
    }

    private void saveAll() {
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel panel = extractPanelFromFrame(frame);
            if (panel != null) {
                panel.saveNow();
            }
        }
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            EditorTabPanel panel = extractPanel(comp);
            if (panel != null) {
                panel.saveNow();
            }
        }
        persistOpenFrames();
    }

    private void openManageDialog() {
        ManageNotesDialog dialog = new ManageNotesDialog(this, noteRepository, this::openNoteInCurrentMode);
        dialog.setVisible(true);
    }

    private void importNoteFromFile() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("导入本地笔记 (*.sql, *.md)");
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("SQL/Markdown", "sql", "md", "txt"));
        int result = chooser.showOpenDialog(this);
        if (result != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path path = chooser.getSelectedFile().toPath();
        String baseName = path.getFileName().toString();
        int dot = baseName.lastIndexOf('.') > 0 ? baseName.lastIndexOf('.') : baseName.length();
        String title = baseName.substring(0, dot);
        DatabaseType type = (DatabaseType) JOptionPane.showInputDialog(this, "选择数据库类型", "导入笔记",
                JOptionPane.PLAIN_MESSAGE, null, DatabaseType.values(), DatabaseType.POSTGRESQL);
        if (type == null) return;
        title = askUniqueTitle(title);
        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            Note note = noteRepository.create(title, type);
            noteRepository.updateContent(note, content);
            openNoteInCurrentMode(note);
        } catch (IllegalStateException ignore) {
            // 用户取消输入
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "导入失败: " + ex.getMessage());
        }
    }

    private String askUniqueTitle(String suggested) {
        String title = suggested;
        while (noteRepository.titleExists(title, null)) {
            title = JOptionPane.showInputDialog(this, "笔记标题已存在，请输入新标题", title + "_1");
            if (title == null || title.isBlank()) {
                throw new IllegalStateException("用户取消导入");
            }
        }
        return title;
    }

    private void openEditorSettings() {
        SqlEditorSettingsDialog dialog = new SqlEditorSettingsDialog(this, convertFullWidth);
        dialog.setVisible(true);
        if (dialog.isConfirmed()) {
            convertFullWidth = dialog.isConvertFullWidthEnabled();
            appStateRepository.saveFullWidthOption(convertFullWidth);
            panelCache.values().forEach(p -> p.setFullWidthConversionEnabled(convertFullWidth));
        }
    }

    private EditorTabPanel extractPanel(Component comp) {
        if (comp instanceof EditorTabPanel) {
            return (EditorTabPanel) comp;
        }
        return null;
    }

    private EditorTabPanel extractPanelFromFrame(JInternalFrame frame) {
        if (frame == null) return null;
        if (frame.getContentPane().getComponentCount() == 0) return null;
        return extractPanel(frame.getContentPane().getComponent(0));
    }

    private void updateTabTitle(EditorTabPanel panel, String title) {
        int idx = tabbedPane.indexOfComponent(panel);
        if (idx >= 0) {
            tabbedPane.setTitleAt(idx, title);
        }
    }

    private void updateTitleForPanel(String title, Long noteId) {
        long targetId = noteId != null ? noteId : -1L;
        panelCache.values().forEach(p -> {
            if (p.getNote().getId() == targetId) {
                updateTabTitle(p, title);
                for (JInternalFrame frame : desktopPane.getAllFrames()) {
                    EditorTabPanel framePanel = extractPanelFromFrame(frame);
                    if (framePanel == p) {
                        frame.setTitle(title);
                        break;
                    }
                }
            }
        });
        persistOpenFrames();
    }

    private void persistOpenFrames() {
        java.util.Set<Long> ids = new java.util.LinkedHashSet<>();
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel panel = extractPanelFromFrame(frame);
            if (panel != null) {
                ids.add(panel.getNote().getId());
            }
        }
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            EditorTabPanel panel = extractPanel(comp);
            if (panel != null) {
                ids.add(panel.getNote().getId());
            }
        }
        appStateRepository.saveOpenNotes(new java.util.ArrayList<>(ids));
    }

    private void tileFrames() {
        JInternalFrame[] frames = desktopPane.getAllFrames();
        int count = frames.length;
        if (count == 0) return;
        int rows = (int) Math.ceil(Math.sqrt(count));
        int cols = (int) Math.ceil((double) count / rows);
        int w = desktopPane.getWidth() / cols;
        int h = desktopPane.getHeight() / rows;
        for (int i = 0; i < count; i++) {
            int r = i / cols;
            int c = i % cols;
            try {
                frames[i].setMaximum(false);
            } catch (Exception ignored) {}
            frames[i].setBounds(c * w, r * h, w, h);
        }
    }

    private void updateAutosaveTime(String text) {
        SwingUtilities.invokeLater(() -> autosaveLabel.setText("自动保存: " + text));
    }

    private void updateTaskCount(int count) {
        SwingUtilities.invokeLater(() -> taskLabel.setText("后台任务: " + count));
    }

    private void switchToPanelMode() {
        if (!windowMode) return;
        windowMode = false;
        windowModeItem.setSelected(false);
        panelModeItem.setSelected(true);
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            if (frame.getContentPane().getComponentCount() > 0 && frame.getContentPane().getComponent(0) instanceof EditorTabPanel panel) {
                addTab(panel);
            }
            frame.dispose();
        }
        centerLayout.show(centerPanel, "panel");
        persistOpenFrames();
    }

    private void switchToWindowMode() {
        if (windowMode) return;
        windowMode = true;
        windowModeItem.setSelected(true);
        panelModeItem.setSelected(false);
        java.util.List<EditorTabPanel> panels = new java.util.ArrayList<>();
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            if (comp instanceof EditorTabPanel panel) {
                panels.add(panel);
            }
        }
        tabbedPane.removeAll();
        int offset = 0;
        for (EditorTabPanel panel : panels) {
            detachFromParent(panel);
            JInternalFrame frame = new JInternalFrame(panel.getNote().getTitle(), true, true, true, true);
            frame.setSize(600, 400);
            frame.setLocation(20 * offset, 20 * offset);
            offset++;
            frame.setVisible(true);
            frame.add(panel, BorderLayout.CENTER);
            installRenameHandler(frame, panel);
            desktopPane.add(frame);
            try { frame.setSelected(true); } catch (java.beans.PropertyVetoException ignored) {}
        }
        centerLayout.show(centerPanel, "window");
        persistOpenFrames();
    }
}
