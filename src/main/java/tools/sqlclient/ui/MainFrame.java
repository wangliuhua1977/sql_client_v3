package tools.sqlclient.ui;

import tools.sqlclient.db.AppStateRepository;
import tools.sqlclient.db.EditorStyleRepository;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;
import tools.sqlclient.ui.QueryResultPanel;

import javax.swing.*;
import javax.swing.BorderFactory;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    private final JPanel sharedResultsWrapper = new JPanel(new BorderLayout());
    private final SharedResultView sharedResultView = new SharedResultView();
    private final SQLiteManager sqliteManager;
    private final NoteRepository noteRepository;
    private final AppStateRepository appStateRepository;
    private final MetadataService metadataService;
    private final EditorStyleRepository styleRepository;
    private final AtomicInteger untitledIndex = new AtomicInteger(1);
    private final java.util.Map<Long, EditorTabPanel> panelCache = new java.util.HashMap<>();
    private final Map<Long, java.util.List<CompletableFuture<?>>> runningExecutions = new ConcurrentHashMap<>();
    private final SqlExecutionService sqlExecutionService = new SqlExecutionService();
    private final Map<Long, java.util.concurrent.atomic.AtomicInteger> resultTabCounters = new java.util.HashMap<>();
    private boolean windowMode = true;
    private JRadioButtonMenuItem windowModeItem;
    private JRadioButtonMenuItem panelModeItem;
    private boolean convertFullWidth = true;
    private EditorStyle currentStyle;
    private JButton executeButton;
    private JButton stopButton;
    private JSplitPane mainSplitPane;
    private int sharedDividerLocation = -1;
    private JScrollPane sharedResultsScroll;

    public MainFrame() {
        super("SQL Notebook - 多标签 PG/Hive");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());

        java.nio.file.Path dbPath = java.nio.file.Path.of("metadata.db");
        this.sqliteManager = new SQLiteManager(dbPath);
        this.metadataService = new MetadataService(dbPath);
        this.noteRepository = new NoteRepository(sqliteManager);
        this.appStateRepository = new AppStateRepository(sqliteManager);
        this.styleRepository = new EditorStyleRepository(sqliteManager);
        var styles = styleRepository.listAll();
        String styleName = appStateRepository.loadCurrentStyleName(styles.isEmpty() ? "默认" : styles.get(0).getName());
        this.currentStyle = styles.stream().filter(s -> s.getName().equals(styleName)).findFirst()
                .orElseGet(() -> {
                    if (!styles.isEmpty()) {
                        return styles.get(0);
                    }
                    return new EditorStyle(
                            "默认",
                            14,
                            "#FFFFFF", // background
                            "#000000", // foreground
                            "#CCE8FF", // selection
                            "#000000", // caret
                            "#005CC5", // keyword
                            "#032F62", // string
                            "#6A737D", // comment
                            "#1C7C54", // number
                            "#000000", // operator
                            "#6F42C1", // function
                            "#005CC5", // data type
                            "#24292E", // identifier
                            "#D73A49", // literal
                            "#F6F8FA", // line highlight
                            "#FFDD88"  // bracket
                    );
                });
        this.convertFullWidth = appStateRepository.loadFullWidthOption(true);
        buildMenu();
        buildToolbar();
        buildContent();
        buildStatusBar();
        metadataService.refreshMetadataAsync(() -> {});
        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent e) {
                persistOpenFrames();
            }
        });
        updateExecutionButtons();
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

        tools.add(new JMenuItem(new AbstractAction("重置元数据") {
            @Override
            public void actionPerformed(ActionEvent e) {
                int option = JOptionPane.showConfirmDialog(MainFrame.this,
                        "重置将清空本地表/函数/存储过程/字段元数据，使用频次也会被清空。是否继续？",
                        "重置元数据",
                        JOptionPane.YES_NO_OPTION,
                        JOptionPane.WARNING_MESSAGE);
                if (option == JOptionPane.YES_OPTION) {
                    metadataService.resetMetadataAsync(() -> JOptionPane.showMessageDialog(MainFrame.this,
                            "已重置并重新获取元数据。",
                            "完成",
                            JOptionPane.INFORMATION_MESSAGE));
                }
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

    private void buildToolbar() {
        JToolBar toolBar = new JToolBar();
        toolBar.setFloatable(false);
        executeButton = new JButton(createRunIcon());
        executeButton.setToolTipText("执行 (Ctrl+Enter)");
        stopButton = new JButton(createStopIcon());
        stopButton.setToolTipText("停止执行");
        stopButton.setEnabled(false);
        executeButton.addActionListener(e -> executeCurrentSql());
        stopButton.addActionListener(e -> stopCurrentExecution());
        styleToolbarButton(executeButton);
        styleToolbarButton(stopButton);
        toolBar.add(executeButton);
        toolBar.add(stopButton);
        add(toolBar, BorderLayout.NORTH);
    }

    private void buildContent() {
        centerPanel.setMinimumSize(new Dimension(200, 240));
        centerPanel.add(desktopPane, "window");
        centerPanel.add(tabbedPane, "panel");
        sharedResultsScroll = new JScrollPane(sharedResultView.wrapper);
        sharedResultsScroll.setVisible(false);
        sharedResultsWrapper.add(sharedResultsScroll, BorderLayout.CENTER);
        sharedResultsWrapper.setMinimumSize(new Dimension(100, 180));
        sharedResultsWrapper.setVisible(false);
        desktopPane.addPropertyChangeListener("selectedFrame", evt -> updateExecutionButtons());
        tabbedPane.addChangeListener(e -> updateExecutionButtons());
        mainSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, centerPanel, sharedResultsWrapper);
        mainSplitPane.setResizeWeight(1.0);
        mainSplitPane.setDividerSize(10);
        mainSplitPane.setOneTouchExpandable(false);
        mainSplitPane.setContinuousLayout(true);
        mainSplitPane.setBorder(BorderFactory.createEmptyBorder());
        mainSplitPane.addPropertyChangeListener(JSplitPane.DIVIDER_LOCATION_PROPERTY, evt -> {
            if (mainSplitPane.getHeight() <= 0) return;
            int minTop = centerPanel.getMinimumSize() != null ? centerPanel.getMinimumSize().height : 200;
            int minBottom = sharedResultsWrapper.getMinimumSize().height;
            int location = Math.max(minTop, Math.min(mainSplitPane.getDividerLocation(), mainSplitPane.getHeight() - minBottom));
            sharedDividerLocation = location;
        });
        add(mainSplitPane, BorderLayout.CENTER);
        SwingUtilities.invokeLater(this::collapseSharedResults);
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
        while (true) {
            String suggested = "未命名" + untitledIndex.getAndIncrement();
            String title = JOptionPane.showInputDialog(this, "输入笔记名称", suggested);
            if (title == null) {
                return;
            }
            title = title.trim();
            if (title.isEmpty()) {
                JOptionPane.showMessageDialog(this, "标题不能为空");
                continue;
            }
            if (noteRepository.titleExists(title, null)) {
                JOptionPane.showMessageDialog(this, "标题已存在，请重新输入");
                continue;
            }
            Note note = noteRepository.create(title, type);
            openNoteInCurrentMode(note);
            break;
        }
    }

    private EditorTabPanel getOrCreatePanel(Note note) {
        return panelCache.computeIfAbsent(note.getId(), id -> {
            EditorTabPanel p = new EditorTabPanel(noteRepository, metadataService,
                    this::updateAutosaveTime, this::updateTaskCount,
                    newTitle -> updateTitleForPanel(newTitle, id), note, convertFullWidth, currentStyle);
            p.setExecuteHandler(() -> executeCurrentSql(true));
            return p;
        });
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
        updateExecutionButtons();
    }

    private void addTab(EditorTabPanel panel) {
        detachFromParent(panel);
        tabbedPane.addTab(panel.getNote().getTitle(), panel);
        int idx = tabbedPane.indexOfComponent(panel);
        tabbedPane.setSelectedIndex(idx);
        persistOpenFrames();
        updateExecutionButtons();
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
                public void mousePressed(MouseEvent e) {
                    if (SwingUtilities.isRightMouseButton(e)) {
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

    private void executeCurrentSql() {
        executeCurrentSql(false);
    }

    private void executeCurrentSql(boolean blockMode) {
        EditorTabPanel panel = getCurrentPanel();
        if (panel == null) {
            JOptionPane.showMessageDialog(this, "请先打开一个笔记窗口");
            return;
        }
        long noteId = panel.getNote().getId();
        if (runningExecutions.getOrDefault(noteId, java.util.List.of()).size() > 0) {
            JOptionPane.showMessageDialog(this, "当前窗口正在执行，请先停止或等待结束");
            return;
        }
        java.util.List<String> statements = panel.getExecutableStatements(blockMode);
        if (statements.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请选择要执行的 SQL 语句");
            return;
        }
        if (windowMode) {
            panel.clearLocalResults();
        } else {
            SharedResultView target = ensureSharedView();
            target.clear();
            collapseSharedResults();
        }
        resultTabCounters.put(noteId, new AtomicInteger(1));
        statusLabel.setText("执行中...");
        panel.setExecutionRunning(true);
        runningExecutions.putIfAbsent(noteId, new CopyOnWriteArrayList<>());
        for (String stmt : statements) {
            CompletableFuture<Void> future = sqlExecutionService.execute(stmt,
                    res -> SwingUtilities.invokeLater(() -> renderResult(noteId, panel, res)),
                    ex -> SwingUtilities.invokeLater(() -> renderError(noteId, panel, stmt, ex.getMessage())));
            runningExecutions.get(noteId).add(future);
            future.whenComplete((v, ex) -> SwingUtilities.invokeLater(() -> {
                java.util.List<CompletableFuture<?>> list = runningExecutions.get(noteId);
                if (list != null) {
                    list.remove(future);
                    if (list.isEmpty()) {
                        runningExecutions.remove(noteId);
                        panel.setExecutionRunning(false);
                    }
                }
                updateExecutionButtons();
                statusLabel.setText("就绪");
            }));
        }
        updateExecutionButtons();
    }

    private void renderResult(long noteId, EditorTabPanel panel, SqlExecResult result) {
        int idx = nextResultIndex(noteId);
        String tabTitle = windowMode ? "结果" + idx : panel.getNote().getTitle() + "-结果" + idx;
        QueryResultPanel qp = new QueryResultPanel(result, result.getSql());
        if (windowMode) {
            panel.addLocalResultPanel(tabTitle, qp, result.getSql());
        } else {
            SharedResultView view = ensureSharedView();
            view.addResultTab(tabTitle, qp, result.getSql());
            expandSharedResults();
        }
    }

    private int nextResultIndex(long noteId) {
        return resultTabCounters.computeIfAbsent(noteId, k -> new AtomicInteger(1)).getAndIncrement();
    }

    private void renderError(long noteId, EditorTabPanel panel, String sql, String message) {
        JPanel error = new JPanel(new BorderLayout());
        error.setBorder(javax.swing.BorderFactory.createTitledBorder("执行失败"));
        error.setToolTipText(sql);
        error.add(new JLabel(message), BorderLayout.CENTER);
        String tabTitle = windowMode ? "错误" + nextResultIndex(noteId)
                : panel.getNote().getTitle() + "-错误" + nextResultIndex(noteId);
        if (windowMode) {
            panel.addLocalResultPanel(tabTitle, error, sql);
        } else {
            SharedResultView view = ensureSharedView();
            view.addResultTab(tabTitle, error, sql);
            expandSharedResults();
        }
    }

    private SharedResultView ensureSharedView() {
        return sharedResultView;
    }

    private void expandSharedResults() {
        if (sharedResultsScroll != null) {
            sharedResultsScroll.setVisible(true);
        }
        sharedResultsWrapper.setVisible(true);
        if (mainSplitPane == null) return;
        int height = mainSplitPane.getHeight();
        int minTop = centerPanel.getMinimumSize() != null ? centerPanel.getMinimumSize().height : 200;
        int minBottom = sharedResultsWrapper.getMinimumSize().height;
        int target = sharedDividerLocation > 0 ? sharedDividerLocation : (int) (height * 0.7);
        int maxLocation = Math.max(minTop, height - minBottom);
        int clamped = Math.max(minTop, Math.min(target, maxLocation));
        SwingUtilities.invokeLater(() -> mainSplitPane.setDividerLocation(clamped));
    }

    private void collapseSharedResults() {
        if (mainSplitPane == null) return;
        if (sharedResultsScroll != null) {
            sharedResultsScroll.setVisible(false);
        }
        sharedResultsWrapper.setVisible(false);
        int height = mainSplitPane.getHeight();
        int minTop = centerPanel.getMinimumSize() != null ? centerPanel.getMinimumSize().height : 200;
        int minBottom = sharedResultsWrapper.getMinimumSize().height;
        int collapsePos = sharedResultsWrapper.isVisible() ? Math.max(minTop, height - minBottom) : height;
        SwingUtilities.invokeLater(() -> mainSplitPane.setDividerLocation(collapsePos));
    }

    private static class SharedResultView {
        final JPanel wrapper = new JPanel(new BorderLayout());
        final JTabbedPane tabs = new JTabbedPane();
        final JScrollPane scroll;

        SharedResultView() {
            tabs.setBorder(BorderFactory.createEmptyBorder());
            scroll = new JScrollPane(tabs);
            wrapper.setBorder(BorderFactory.createTitledBorder("结果集"));
            wrapper.add(scroll, BorderLayout.CENTER);
        }

        void addResultTab(String title, JComponent comp, String hint) {
            tabs.addTab(title, comp);
            int idx = tabs.indexOfComponent(comp);
            if (idx >= 0) {
                tabs.setToolTipTextAt(idx, hint);
            }
            tabs.setVisible(true);
            tabs.setSelectedComponent(comp);
        }

        void clear() {
            tabs.removeAll();
            tabs.setVisible(false);
        }
    }

    private void stopCurrentExecution() {
        EditorTabPanel panel = getCurrentPanel();
        if (panel == null) return;
        long noteId = panel.getNote().getId();
        java.util.List<CompletableFuture<?>> list = runningExecutions.remove(noteId);
        if (list != null) {
            for (CompletableFuture<?> f : list) {
                f.cancel(true);
            }
        }
        panel.setExecutionRunning(false);
        updateExecutionButtons();
        statusLabel.setText("就绪");
    }

    private Icon createRunIcon() {
        return new Icon() {
            private final int size = 14;

            @Override
            public void paintIcon(Component c, Graphics g, int x, int y) {
                Graphics2D g2 = (Graphics2D) g.create();
                g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                int[] xs = {x, x, x + size};
                int[] ys = {y, y + size, y + size / 2};
                Color color = c.isEnabled() ? new Color(46, 170, 220) : Color.GRAY;
                g2.setColor(color);
                g2.fillPolygon(xs, ys, 3);
                g2.dispose();
            }

            @Override
            public int getIconWidth() { return size + 2; }

            @Override
            public int getIconHeight() { return size + 2; }
        };
    }

    private Icon createStopIcon() {
        return new Icon() {
            private final int size = 12;

            @Override
            public void paintIcon(Component c, Graphics g, int x, int y) {
                Graphics2D g2 = (Graphics2D) g.create();
                Color color = c.isEnabled() ? new Color(46, 170, 220) : Color.GRAY;
                g2.setColor(color);
                g2.fillRect(x, y, size, size);
                g2.dispose();
            }

            @Override
            public int getIconWidth() { return size + 2; }

            @Override
            public int getIconHeight() { return size + 2; }
        };
    }

    private void updateExecutionButtons() {
        EditorTabPanel panel = getCurrentPanel();
        if (panel == null) {
            executeButton.setEnabled(false);
            stopButton.setEnabled(false);
            refreshToolbarButtonStyles();
            return;
        }
        long noteId = panel.getNote().getId();
        boolean running = runningExecutions.getOrDefault(noteId, java.util.List.of()).size() > 0;
        executeButton.setEnabled(!running);
        stopButton.setEnabled(running);
        refreshToolbarButtonStyles();
    }

    private void styleToolbarButton(JButton button) {
        button.setFocusPainted(false);
        button.setContentAreaFilled(false);
        button.setBorder(new javax.swing.border.LineBorder(new Color(46, 170, 220)));
    }

    private void refreshToolbarButtonStyles() {
        Color active = new Color(46, 170, 220);
        if (executeButton != null) {
            executeButton.setBorder(new javax.swing.border.LineBorder(executeButton.isEnabled() ? active : Color.BLACK));
        }
        if (stopButton != null) {
            stopButton.setBorder(new javax.swing.border.LineBorder(stopButton.isEnabled() ? active : Color.BLACK));
        }
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
        SqlEditorSettingsDialog dialog = new SqlEditorSettingsDialog(this, convertFullWidth, styleRepository,
                styleRepository.listAll(), currentStyle);
        dialog.setVisible(true);
        if (dialog.isConfirmed()) {
            convertFullWidth = dialog.isConvertFullWidthEnabled();
            appStateRepository.saveFullWidthOption(convertFullWidth);
            panelCache.values().forEach(p -> p.setFullWidthConversionEnabled(convertFullWidth));
            dialog.getSelectedStyle().ifPresent(style -> {
                currentStyle = style;
                appStateRepository.saveCurrentStyleName(style.getName());
                panelCache.values().forEach(p -> p.applyStyle(style));
            });
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
        sharedResultsWrapper.setVisible(true);
        persistOpenFrames();
        updateExecutionButtons();
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
        sharedResultsWrapper.setVisible(false);
        persistOpenFrames();
        updateExecutionButtons();
    }
}
