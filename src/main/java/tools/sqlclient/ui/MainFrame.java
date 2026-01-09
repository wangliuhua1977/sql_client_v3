package tools.sqlclient.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.AppStateRepository;
import tools.sqlclient.db.EditorStyleRepository;
import tools.sqlclient.db.LocalDatabasePathProvider;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.db.SqlHistoryRepository;
import tools.sqlclient.db.SqlSnippetRepository;
import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.editor.NotePersistenceStrategy;
import tools.sqlclient.editor.PersistentNotePersistenceStrategy;
import tools.sqlclient.editor.TemporaryNotePersistenceStrategy;
import tools.sqlclient.exec.AsyncJobStatus;
import tools.sqlclient.exec.ColumnDef;
import tools.sqlclient.exec.ColumnOrderDecider;
import tools.sqlclient.exec.DatabaseErrorInfo;
import tools.sqlclient.exec.ErrorDisplayFormatter;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionException;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;
import tools.sqlclient.pg.PgRoutineService;
import tools.sqlclient.pg.RoutineInfo;
import tools.sqlclient.ui.TemporaryNoteWindow;
import tools.sqlclient.ui.ThemeManager;
import tools.sqlclient.ui.ThemeOption;
import tools.sqlclient.ui.QueryResultPanel;
import tools.sqlclient.ui.ManageNotesDialog.SearchNavigation;
import tools.sqlclient.util.Config;
import tools.sqlclient.util.IconFactory;
import tools.sqlclient.util.LinkResolver;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.UntitledNoteNamer;
import tools.sqlclient.ui.DockManager;
import tools.sqlclient.ui.DockPosition;
import tools.sqlclient.ui.LayoutState;
import tools.sqlclient.ui.LogPanel;

import javax.swing.*;
import javax.swing.BorderFactory;
import javax.swing.border.EmptyBorder;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.event.InternalFrameEvent;
import javax.swing.plaf.basic.BasicInternalFrameUI;
import javax.swing.plaf.FontUIResource;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 主窗口，符合 Windows 11 扁平化风格，全部中文。
 */
public class MainFrame extends JFrame {
    private static final Logger log = LoggerFactory.getLogger(MainFrame.class);
    private final CardLayout centerLayout = new CardLayout();
    private final JPanel centerPanel = new JPanel(centerLayout);
    private final JDesktopPane desktopPane = new JDesktopPane();
    private final JTabbedPane tabbedPane = new JTabbedPane();
    private final JLabel statusLabel = new JLabel("就绪");
    private final JLabel autosaveLabel = new JLabel("自动保存: -");
    private final JLabel taskLabel = new JLabel("后台任务: 0");
    private final JLabel suggestionStateLabel = new JLabel("联想: 开启");
    private final JLabel metadataLabel = new JLabel("元数据: -");
    private final JPanel sharedResultsWrapper = new JPanel(new BorderLayout());
    private final SharedResultView sharedResultView = new SharedResultView();
    private final SQLiteManager sqliteManager;
    private final NoteRepository noteRepository;
    private final AppStateRepository appStateRepository;
    private final SqlSnippetRepository sqlSnippetRepository;
    private final SqlHistoryRepository sqlHistoryRepository;
    private final MetadataService metadataService;
    private final EditorStyleRepository styleRepository;
    private final ThemeManager themeManager = new ThemeManager();
    private final java.util.Map<Long, EditorTabPanel> panelCache = new java.util.HashMap<>();
    private final Map<Long, java.util.List<RunningJobHandle>> runningExecutions = new ConcurrentHashMap<>();
    private final SqlExecutionService sqlExecutionService = new SqlExecutionService();
    private final PgRoutineService pgRoutineService = new PgRoutineService(sqlExecutionService);
    private final ColumnOrderDecider columnOrderDecider;
    private final Map<Long, java.util.concurrent.atomic.AtomicInteger> resultTabCounters = new java.util.HashMap<>();
    private final Map<Long, EditorTabPanel> routineTabIndex = new java.util.HashMap<>();
    private final Font uiBaseFont = new Font(Font.SANS_SERIF, Font.PLAIN, 13);
    // 笔记图标持久化：noteId -> 图标规格
    private final java.util.Map<Long, NoteIconSpec> noteIconSpecs = new java.util.HashMap<>();
    private final java.util.Random iconRandom = new java.util.Random();
    private boolean windowMode = true;
    private JRadioButtonMenuItem windowModeItem;
    private JRadioButtonMenuItem panelModeItem;
    private boolean convertFullWidth = true;
    private EditorStyle currentStyle;
    private List<EditorStyle> availableStyles = new java.util.ArrayList<>();
    private ThemeOption currentTheme;
    private boolean suggestionEnabled = true;
    private EditorTabPanel activePanel;
    private JButton executeButton;
    private JButton stopButton;
    private JSplitPane leftSplit;
    private JSplitPane centerRightSplit;
    private int sharedDividerLocation = -1;
    private JScrollPane sharedResultsScroll;
    private LogPanel logPanel;
    private DockManager dockManager;
    private LayoutState layoutState;
    private JTabbedPane bottomTabbedPane;
    private JTabbedPane rightTabbedPane;
    private JPanel leftPanel;
    private JPanel rightPanel;
    private JPanel statusBar;
    private JTabbedPane resultTabs;
    private JTextArea messageArea;
    private JPanel auxiliaryPanel;
    private JTabbedPane auxiliaryTabs;
    private JComponent historyPanel;
    private JComponent inspectorPanel;
    private ObjectBrowserDialog objectBrowserDialog;
    private ObjectBrowserPanel navigationBrowserPanel;
    private JTree navigationTree;
    private boolean leftVisible = true;
    private boolean rightVisible = false;
    private boolean bottomVisible = true;
    private boolean editorMaximized = false;
    private Rectangle floatingLogBounds;
    private int lastLeftDividerLocation = -1;
    private int lastRightDividerLocation = -1;
    private int lastBottomDividerLocation = -1;
    private boolean savedLeftVisibleForMax = true;
    private boolean savedRightVisibleForMax = false;
    private boolean savedBottomVisibleForMax = true;
    private boolean debugMode = false;
    private static final String DEBUG_SECRET = "yy181911a";
    private static final String RESTORE_SECRET = "我决定今天请小胖哥吃饭喝咖啡";
    private static final Pattern LINE_PATTERN = Pattern.compile("LINE\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern POSITION_PATTERN = Pattern.compile("Position:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    // 记住上一次备份/恢复使用的目录
    private static final String BACKUP_DIR_STATE_KEY = "last_backup_dir";
    private final ScheduledExecutorService scheduledBackupExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> scheduledBackupTask;
    private Path scheduledBackupDir;
    private String scheduledBackupLabel;
    private Path lastBackupDir;
    private int defaultPageSize;

    private QuickSqlSnippetDialog quickSqlSnippetDialog;
    private ExecutionHistoryDialog executionHistoryDialog;
    private TrashBinDialog trashBinDialog;
    private int focusCycleIndex;

    public MainFrame() {
        // 需求 2：修改主窗体标题
        super("Supper SQL Note");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());
        this.layoutState = new LayoutState();
        Rectangle restored = layoutState.loadWindowBounds(getBounds());
        if (restored != null) {
            setBounds(restored);
        }
        editorMaximized = layoutState.isEditorMaximized(false);
        leftVisible = layoutState.isLeftVisible(true);
        rightVisible = layoutState.isRightVisible(false);
        bottomVisible = layoutState.isBottomVisible(true);
        floatingLogBounds = layoutState.loadLogFloatingBounds();

        java.nio.file.Path dbPath = LocalDatabasePathProvider.resolveMetadataDbPath();
        this.sqliteManager = new SQLiteManager(dbPath);
        this.metadataService = new MetadataService(dbPath);
        this.noteRepository = new NoteRepository(sqliteManager);
        this.appStateRepository = new AppStateRepository(sqliteManager);
        this.sqlSnippetRepository = new SqlSnippetRepository(sqliteManager);
        this.sqlHistoryRepository = new SqlHistoryRepository(sqliteManager);
        this.styleRepository = new EditorStyleRepository(sqliteManager);
        this.columnOrderDecider = new ColumnOrderDecider(metadataService);
        this.defaultPageSize = sanitizePageSize(appStateRepository.loadDefaultPageSize(Config.getDefaultPageSize()));
        Config.overrideDefaultPageSize(defaultPageSize);
        initTheme();
        // 加载笔记图标配置（noteId -> 图标规格）
        loadNoteIcons();
        // 加载主窗体图标：resources 目录下的 PNG，不存在则回退默认图标
        loadMainWindowIcon();

        ensureBuiltinStyles();
        availableStyles = styleRepository.listAll();
        String styleName = appStateRepository.loadCurrentStyleName(
                availableStyles.isEmpty() ? "默认" : availableStyles.get(0).getName());
        this.currentStyle = findStyleByName(styleName)
                .orElseGet(() -> availableStyles.isEmpty() ? new EditorStyle(
                        "默认",
                        14,
                        "#FFFFFF",
                        "#000000",
                        "#CCE8FF",
                        "#000000",
                        "#005CC5",
                        "#032F62",
                        "#6A737D",
                        "#1C7C54",
                        "#000000",
                        "#6F42C1",
                        "#005CC5",
                        "#24292E",
                        "#D73A49",
                        "#F6F8FA",
                        "#FFDD88"
                ) : availableStyles.get(0));
        this.convertFullWidth = appStateRepository.loadFullWidthOption(true);

        // 需求 1：加载上一次备份目录
        String lastDir = appStateRepository.loadStringOption(BACKUP_DIR_STATE_KEY, "");
        if (lastDir != null && !lastDir.isBlank()) {
            try {
                Path p = Paths.get(lastDir).toAbsolutePath().normalize();
                if (Files.isDirectory(p)) {
                    lastBackupDir = p;
                }
            } catch (Exception ignored) {
            }
        }

        buildMenu();
        buildToolbar();
        buildContent();
        buildStatusBar();
        OperationLog.setAppender(line -> logPanel.appendLine(line));
        metadataLabel.setText("元数据: " + metadataService.countCachedObjects());
        metadataService.syncMetadataOnStartup(this::handleMetadataRefreshResult);
        installGlobalShortcuts();
        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent e) {
                persistOpenFrames();
                cancelScheduledBackup();
                scheduledBackupExecutor.shutdownNow();
                persistLayoutState();
            }

            @Override
            public void windowDeactivated(java.awt.event.WindowEvent e) {
                if (activePanel != null) {
                    activePanel.hideSuggestionPopup("windowDeactivated");
                }
            }
        });
        updateExecutionButtons();
    }

    // ==================== 主窗体图标 ====================

    /**
     * 从 resources 目录加载主窗体图标：
     * 约定路径：/AI.png
     * 如果资源不存在或读取失败，则回退为系统默认图标并记录 warn。
     */
    private void loadMainWindowIcon() {
        try {
            URL url = getClass().getResource("/AI.png");
            if (url != null) {
                BufferedImage img = javax.imageio.ImageIO.read(url);
                if (img != null) {
                    setIconImage(img);
                    return;
                }
            }
            Icon uiIcon = UIManager.getIcon("FileView.computerIcon");
            if (uiIcon instanceof ImageIcon icon) {
                setIconImage(icon.getImage());
            }
            log.warn("主窗体图标加载失败或资源缺失: /AI.png");
        } catch (Exception e) {
            log.warn("主窗体图标加载异常", e);
        }
    }

    // ==================== 笔记图标：加载 / 保存 / 生成 ====================

    /**
     * 启动时从本地文件加载所有笔记图标配置。
     * 文件格式：每行  noteId|paletteIndex,shapeIndex,motifIndex
     */
    private void loadNoteIcons() {
        try {
            java.nio.file.Path path = java.nio.file.Path.of("note_icons.conf");
            if (!java.nio.file.Files.exists(path)) {
                return;
            }
            java.util.List<String> lines =
                    java.nio.file.Files.readAllLines(path, java.nio.charset.StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line == null || line.isBlank()) continue;
                String[] parts = line.split("\\|", 2);
                if (parts.length != 2) continue;
                long id = Long.parseLong(parts[0].trim());
                NoteIconSpec spec = NoteIconSpec.fromString(parts[1].trim());
                if (spec != null) {
                    noteIconSpecs.put(id, spec);
                }
            }
        } catch (Exception ignore) {
            // 图标加载失败不用影响主流程，后面会重新生成
        }
    }

    /**
     * 将当前所有笔记图标配置写入本地文件 note_icons.conf。
     */
    private void persistNoteIcons() {
        try {
            java.nio.file.Path path = java.nio.file.Path.of("note_icons.conf");
            java.util.List<String> out = new java.util.ArrayList<>();
            for (java.util.Map.Entry<Long, NoteIconSpec> e : noteIconSpecs.entrySet()) {
                if (e.getKey() != null && e.getKey() > 0) {
                    out.add(e.getKey() + "|" + e.getValue().toConfigString());
                }
            }
            java.nio.file.Files.write(path, out, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception ignore) {
            // 写失败就算了，不阻塞主流程
        }
    }

    /**
     * 获取某个笔记的图标，如无则生成一个随机图标规格并持久化。
     */
    private Icon getOrCreateNoteIcon(Note note) {
        if (note == null) return null;
        long id = note.getId();
        NoteIconSpec spec = noteIconSpecs.get(id);
        if (spec == null) {
            spec = NoteIconSpec.random(iconRandom);
            noteIconSpecs.put(id, spec);
            if (!note.isTemporary()) {
                persistNoteIcons();
            }
        }
        return spec.toIcon();
    }

    /**
     * 重新抽一枚图标：管理笔记中“更换图标”会调用。
     * 同时刷新已打开窗口和标签上的图标。
     */
    private Icon regenerateNoteIcon(Note note) {
        if (note == null) return null;
        long id = note.getId();
        NoteIconSpec spec = NoteIconSpec.random(iconRandom);
        noteIconSpecs.put(id, spec);
        if (!note.isTemporary()) {
            persistNoteIcons();
        }
        Icon icon = spec.toIcon();
        // 刷新所有已打开的窗口 & 标签
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel p = extractPanelFromFrame(frame);
            if (p != null && p.getNote().getId() == id) {
                frame.setFrameIcon(icon);
            }
        }
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            EditorTabPanel p = extractPanel(comp);
            if (p != null && p.getNote().getId() == id) {
                tabbedPane.setIconAt(i, icon);
            }
        }
        return icon;
    }

    /**
     * 图标规格：决定调色板、外形、内部图案。
     * 现有设计：12 组高饱和配色 × 8 种外形 × 10 种内部图案
     * 理论组合数 960 种，差异明显、不易混淆。
     */
    private static class NoteIconSpec {
        // 颜色调色板：每组 [0]=背景, [1]=前景主色, [2]=点缀色
        private static final java.awt.Color[][] PALETTES = new java.awt.Color[][]{
                // 高饱和撞色，避免“看上去都差不多”
                {new java.awt.Color(0xF44336), new java.awt.Color(0xFFFFFF), new java.awt.Color(0xFF9800)}, // 红 + 白 + 橙
                {new java.awt.Color(0xE91E63), new java.awt.Color(0xFFFFFF), new java.awt.Color(0xFFC107)}, // 粉 + 白 + 金
                {new java.awt.Color(0x9C27B0), new java.awt.Color(0xFFEB3B), new java.awt.Color(0xFFFFFF)}, // 紫 + 黄 + 白
                {new java.awt.Color(0x673AB7), new java.awt.Color(0xFFEB3B), new java.awt.Color(0x00BCD4)}, // 深紫 + 黄 + 青
                {new java.awt.Color(0x3F51B5), new java.awt.Color(0xFFFFFF), new java.awt.Color(0xFF5722)}, // 靛蓝 + 白 + 橘
                {new java.awt.Color(0x2196F3), new java.awt.Color(0xFFFFFF), new java.awt.Color(0xFFEB3B)}, // 亮蓝 + 白 + 黄
                {new java.awt.Color(0x009688), new java.awt.Color(0xFFFFFF), new java.awt.Color(0xFF9800)}, // 蓝绿 + 白 + 橙
                {new java.awt.Color(0x4CAF50), new java.awt.Color(0xFFFFFF), new java.awt.Color(0xFF5722)}, // 绿色 + 白 + 深橘
                {new java.awt.Color(0x8BC34A), new java.awt.Color(0x1A237E), new java.awt.Color(0xFF5722)}, // 亮绿 + 深蓝 + 深橘
                {new java.awt.Color(0xFFC107), new java.awt.Color(0x1A237E), new java.awt.Color(0xD32F2F)}, // 金黄 + 深蓝 + 深红
                {new java.awt.Color(0xFF9800), new java.awt.Color(0x212121), new java.awt.Color(0x03A9F4)}, // 橙色 + 深灰 + 天蓝
                {new java.awt.Color(0xFF5722), new java.awt.Color(0xFFFFFF), new java.awt.Color(0x00BCD4)}  // 深橘 + 白 + 青
        };

        // 外形种类数量
        private static final int SHAPE_COUNT = 8;
        // 内部图案种类数量
        private static final int MOTIF_COUNT = 10;

        final int paletteIndex;
        final int shapeIndex;
        final int motifIndex;

        NoteIconSpec(int paletteIndex, int shapeIndex, int motifIndex) {
            this.paletteIndex = paletteIndex;
            this.shapeIndex = shapeIndex;
            this.motifIndex = motifIndex;
        }

        static NoteIconSpec random(java.util.Random rnd) {
            int p = rnd.nextInt(PALETTES.length);
            int s = rnd.nextInt(SHAPE_COUNT);
            int m = rnd.nextInt(MOTIF_COUNT);
            return new NoteIconSpec(p, s, m);
        }

        String toConfigString() {
            return paletteIndex + "," + shapeIndex + "," + motifIndex;
        }

        static NoteIconSpec fromString(String s) {
            if (s == null || s.isBlank()) return null;
            String[] parts = s.split(",");
            if (parts.length < 3) return null;
            try {
                int p = Integer.parseInt(parts[0].trim());
                int sh = Integer.parseInt(parts[1].trim());
                int mo = Integer.parseInt(parts[2].trim());
                if (p < 0 || p >= PALETTES.length) return null;
                if (sh < 0) sh = 0;
                if (mo < 0) mo = 0;
                return new NoteIconSpec(p, sh, mo);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        Icon toIcon() {
            return new NoteIcon(this);
        }
    }

    /**
     * 真正绘制图标的类：高饱和背景 + 多种外形 + 多种内部图案。
     */
    private static class NoteIcon implements Icon {
        private static final int SIZE = 18;
        private final NoteIconSpec spec;

        NoteIcon(NoteIconSpec spec) {
            this.spec = spec;
        }

        @Override
        public int getIconWidth() {
            return SIZE;
        }

        @Override
        public int getIconHeight() {
            return SIZE;
        }

        @Override
        public void paintIcon(Component c, java.awt.Graphics g, int x, int y) {
            java.awt.Graphics2D g2 = (java.awt.Graphics2D) g.create();
            g2.setRenderingHint(java.awt.RenderingHints.KEY_ANTIALIASING,
                    java.awt.RenderingHints.VALUE_ANTIALIAS_ON);

            java.awt.Color[] palette =
                    NoteIconSpec.PALETTES[spec.paletteIndex % NoteIconSpec.PALETTES.length];
            java.awt.Color bg = palette[0];
            java.awt.Color fg = palette[1];
            java.awt.Color accent = palette[2];

            int padding = 2;
            int w = SIZE - padding * 2;
            int h = SIZE - padding * 2;
            int left = x + padding;
            int top = y + padding;
            int centerX = x + SIZE / 2;
            int centerY = y + SIZE / 2;

            // 绘制外形底图
            drawShape(g2, spec.shapeIndex, bg, accent, left, top, w, h, centerX, centerY);

            // 绘制内部图案
            drawMotif(g2, spec.motifIndex, fg, accent, x, y, SIZE);

            g2.dispose();
        }

        private void drawShape(java.awt.Graphics2D g2,
                               int shapeIndex,
                               java.awt.Color bg,
                               java.awt.Color accent,
                               int left, int top, int w, int h,
                               int cx, int cy) {
            shapeIndex = Math.floorMod(shapeIndex, NoteIconSpec.SHAPE_COUNT);

            g2.setStroke(new java.awt.BasicStroke(1.4f));

            switch (shapeIndex) {
                case 0 -> {
                    // 圆角方块
                    java.awt.geom.RoundRectangle2D.Float rr =
                            new java.awt.geom.RoundRectangle2D.Float(
                                    left, top, w, h, 7, 7);
                    g2.setColor(bg);
                    g2.fill(rr);
                    g2.setColor(accent);
                    g2.draw(rr);
                }
                case 1 -> {
                    // 纯圆
                    java.awt.geom.Ellipse2D.Float circle =
                            new java.awt.geom.Ellipse2D.Float(left, top, w, h);
                    g2.setColor(bg);
                    g2.fill(circle);
                }
                case 2 -> {
                    // 胶囊（横向）
                    java.awt.geom.RoundRectangle2D.Float pill =
                            new java.awt.geom.RoundRectangle2D.Float(
                                    left - 2, top + h * 0.15f,
                                    w + 4, h * 0.7f,
                                    h, h);
                    g2.setColor(bg);
                    g2.fill(pill);
                    g2.setColor(accent.darker());
                    g2.draw(pill);
                }
                case 3 -> {
                    // 菱形
                    java.awt.geom.Path2D.Float diamond = new java.awt.geom.Path2D.Float();
                    diamond.moveTo(cx, top);
                    diamond.lineTo(left + w, cy);
                    diamond.lineTo(cx, top + h);
                    diamond.lineTo(left, cy);
                    diamond.closePath();
                    g2.setColor(bg);
                    g2.fill(diamond);
                    g2.setColor(accent);
                    g2.draw(diamond);
                }
                case 4 -> {
                    // 六边形
                    java.awt.geom.Path2D.Float hex = new java.awt.geom.Path2D.Float();
                    double radius = Math.min(w, h) / 2.0;
                    for (int i = 0; i < 6; i++) {
                        double angle = Math.toRadians(60 * i - 30);
                        double px = cx + radius * Math.cos(angle);
                        double py = cy * 1.0 + radius * Math.sin(angle);
                        if (i == 0) {
                            hex.moveTo(px, py);
                        } else {
                            hex.lineTo(px, py);
                        }
                    }
                    hex.closePath();
                    g2.setColor(bg);
                    g2.fill(hex);
                    g2.setColor(accent);
                    g2.draw(hex);
                }
                case 5 -> {
                    // 文件夹卡片
                    java.awt.geom.Path2D.Float folder = new java.awt.geom.Path2D.Float();
                    int tabHeight = (int) (h * 0.35);
                    int tabWidth = (int) (w * 0.55);
                    folder.moveTo(left, top + tabHeight);
                    folder.lineTo(left, top + h);
                    folder.lineTo(left + w, top + h);
                    folder.lineTo(left + w, top);
                    folder.lineTo(left + tabWidth, top);
                    folder.lineTo(left + tabWidth - (int) (w * 0.2), top + tabHeight);
                    folder.closePath();
                    g2.setColor(bg);
                    g2.fill(folder);
                    g2.setColor(accent.darker());
                    g2.draw(folder);
                }
                case 6 -> {
                    // 圆环
                    int r = Math.min(w, h);
                    java.awt.geom.Ellipse2D.Float outer =
                            new java.awt.geom.Ellipse2D.Float(
                                    cx - r / 2f, cy - r / 2f, r, r);
                    java.awt.geom.Ellipse2D.Float inner =
                            new java.awt.geom.Ellipse2D.Float(
                                    cx - r / 3f, cy - r / 3f, r * 2f / 3f, r * 2f / 3f);
                    g2.setColor(bg);
                    g2.fill(outer);
                    g2.setColor(accent);
                    g2.fill(inner);
                }
                case 7 -> {
                    // 右上角切角方块
                    java.awt.geom.Path2D.Float cut = new java.awt.geom.Path2D.Float();
                    int cutSize = (int) (w * 0.35);
                    cut.moveTo(left, top);
                    cut.lineTo(left + w - cutSize, top);
                    cut.lineTo(left + w, top + cutSize);
                    cut.lineTo(left + w, top + h);
                    cut.lineTo(left, top + h);
                    cut.closePath();
                    g2.setColor(bg);
                    g2.fill(cut);
                    g2.setColor(accent);
                    g2.draw(cut);
                }
            }
        }

        private void drawMotif(java.awt.Graphics2D g2,
                               int motifIndex,
                               java.awt.Color fg,
                               java.awt.Color accent,
                               int x, int y, int size) {
            motifIndex = Math.floorMod(motifIndex, NoteIconSpec.MOTIF_COUNT);

            int inner = size - 8;
            int left = x + (size - inner) / 2;
            int top = y + (size - inner) / 2;
            int w = inner;
            int h = inner;
            int cx = x + size / 2;
            int cy = y + size / 2;

            g2.setStroke(new java.awt.BasicStroke(1.3f));

            switch (motifIndex) {
                case 0 -> {
                    // 中心圆点 + 描边
                    java.awt.geom.Ellipse2D.Float outer =
                            new java.awt.geom.Ellipse2D.Float(
                                    cx - w * 0.3f, cy - h * 0.3f,
                                    w * 0.6f, h * 0.6f);
                    java.awt.geom.Ellipse2D.Float innerCircle =
                            new java.awt.geom.Ellipse2D.Float(
                                    cx - w * 0.16f, cy - h * 0.16f,
                                    w * 0.32f, h * 0.32f);
                    g2.setColor(accent);
                    g2.fill(outer);
                    g2.setColor(fg);
                    g2.fill(innerCircle);
                }
                case 1 -> {
                    // 斜向条纹
                    g2.setColor(fg);
                    for (int i = -w; i < w * 2; i += 4) {
                        g2.drawLine(left + i, top, left + i - h, top + h);
                    }
                    g2.setColor(accent);
                    g2.drawLine(left, top + h / 2, left + w, top + h / 2);
                }
                case 2 -> {
                    // 横向三道杠
                    g2.setColor(fg);
                    int gap = h / 5;
                    int barW = (int) (w * 0.75);
                    int startX = cx - barW / 2;
                    for (int i = -1; i <= 1; i++) {
                        int yy = cy + i * gap;
                        g2.drawLine(startX, yy, startX + barW, yy);
                    }
                }
                case 3 -> {
                    // 纵向三道柱
                    g2.setColor(fg);
                    int gap = w / 5;
                    int barH = (int) (h * 0.75);
                    int startY = cy - barH / 2;
                    for (int i = -1; i <= 1; i++) {
                        int xx = cx + i * gap;
                        g2.drawLine(xx, startY, xx, startY + barH);
                    }
                }
                case 4 -> {
                    // 内部小三角
                    java.awt.geom.Path2D.Float tri = new java.awt.geom.Path2D.Float();
                    tri.moveTo(cx, top + h * 0.25);
                    tri.lineTo(left + w * 0.25, top + h * 0.75);
                    tri.lineTo(left + w * 0.75, top + h * 0.75);
                    tri.closePath();
                    g2.setColor(accent);
                    g2.fill(tri);
                    g2.setColor(fg);
                    g2.draw(tri);
                }
                case 5 -> {
                    // 五角星
                    java.awt.geom.Path2D.Float star = new java.awt.geom.Path2D.Float();
                    double outerR = Math.min(w, h) * 0.45;
                    double innerR = outerR * 0.45;
                    for (int i = 0; i < 10; i++) {
                        double angle = Math.PI / 2 + Math.PI / 5 * i;
                        double r = (i % 2 == 0) ? outerR : innerR;
                        double px = cx + r * Math.cos(angle);
                        double py = cy - r * Math.sin(angle);
                        if (i == 0) {
                            star.moveTo(px, py);
                        } else {
                            star.lineTo(px, py);
                        }
                    }
                    star.closePath();
                    g2.setColor(accent);
                    g2.fill(star);
                    g2.setColor(fg);
                    g2.draw(star);
                }
                case 6 -> {
                    // 柱状图
                    g2.setColor(fg);
                    int barW = w / 5;
                    int baseY = top + h;
                    int x1 = left + w / 8;
                    int x2 = left + w / 8 + barW + 2;
                    int x3 = left + w / 8 + (barW + 2) * 2;
                    g2.fillRect(x1, baseY - h / 3, barW, h / 3);
                    g2.fillRect(x2, baseY - h / 2, barW, h / 2);
                    g2.fillRect(x3, baseY - (int) (h * 0.75), barW, (int) (h * 0.75));
                }
                case 7 -> {
                    // 半身人像：圆头 + 肩膀
                    g2.setColor(fg);
                    java.awt.geom.Ellipse2D.Float head =
                            new java.awt.geom.Ellipse2D.Float(
                                    cx - w * 0.18f, top + h * 0.15f,
                                    w * 0.36f, w * 0.36f);
                    g2.fill(head);
                    java.awt.geom.RoundRectangle2D.Float body =
                            new java.awt.geom.RoundRectangle2D.Float(
                                    left + w * 0.18f, top + h * 0.45f,
                                    w * 0.64f, h * 0.4f,
                                    h * 0.4f, h * 0.4f);
                    g2.fill(body);
                }
                case 8 -> {
                    // 音符
                    g2.setColor(fg);
                    int stemH = (int) (h * 0.6);
                    int stemX = cx + w / 6;
                    int stemY = cy - stemH / 2;
                    g2.drawLine(stemX, stemY, stemX, stemY + stemH);
                    java.awt.geom.Ellipse2D.Float noteHead =
                            new java.awt.geom.Ellipse2D.Float(
                                    stemX - w * 0.3f, stemY + stemH * 0.55f,
                                    w * 0.4f, h * 0.28f);
                    g2.fill(noteHead);
                    g2.setColor(accent);
                    g2.drawArc(stemX - w / 3, stemY, w / 2, h / 2, 0, -160);
                }
                case 9 -> {
                    // 代码尖括号 < >
                    g2.setColor(fg);
                    int len = (int) (w * 0.32);
                    int offset = (int) (w * 0.12);

                    // <
                    java.awt.geom.Path2D.Float leftCode = new java.awt.geom.Path2D.Float();
                    leftCode.moveTo(cx - offset, cy);
                    leftCode.lineTo(cx - offset + len / 2.0, cy - len / 2.0);
                    leftCode.moveTo(cx - offset, cy);
                    leftCode.lineTo(cx - offset + len / 2.0, cy + len / 2.0);
                    g2.draw(leftCode);

                    // >
                    java.awt.geom.Path2D.Float rightCode = new java.awt.geom.Path2D.Float();
                    rightCode.moveTo(cx + offset, cy);
                    rightCode.lineTo(cx + offset - len / 2.0, cy - len / 2.0);
                    rightCode.moveTo(cx + offset, cy);
                    rightCode.lineTo(cx + offset - len / 2.0, cy + len / 2.0);
                    g2.draw(rightCode);
                }
            }
        }
    }

    private void buildMenu() {
        JMenuBar menuBar = new JMenuBar();
        JMenu file = new JMenu("文件");
        JMenu edit = new JMenu("编辑");
        JMenu run = new JMenu("运行");
        JMenu view = new JMenu("视图");
        JMenu tools = new JMenu("工具");
        JMenu help = new JMenu("帮助");

        JCheckBoxMenuItem objectBrowserItem = new JCheckBoxMenuItem("显示对象浏览器", leftVisible);
        objectBrowserItem.addActionListener(e -> toggleLeftPanel());
        view.add(objectBrowserItem);

        JCheckBoxMenuItem resultsItem = new JCheckBoxMenuItem("显示结果区", bottomVisible);
        resultsItem.addActionListener(e -> toggleBottomPanel());
        view.add(resultsItem);

        JCheckBoxMenuItem logsItem = new JCheckBoxMenuItem("显示日志面板", rightVisible);
        logsItem.addActionListener(e -> {
            if (logsItem.isSelected()) {
                focusAuxiliaryPanel(logPanel);
            } else {
                hideRightPanel();
            }
        });
        view.add(logsItem);

        JCheckBoxMenuItem historyItem = new JCheckBoxMenuItem("显示历史面板", false);
        historyItem.addActionListener(e -> {
            if (historyItem.isSelected()) {
                focusAuxiliaryPanel(historyPanel);
            } else {
                hideRightPanel();
            }
        });
        view.add(historyItem);

        JCheckBoxMenuItem inspectorItem = new JCheckBoxMenuItem("显示设置面板", false);
        inspectorItem.addActionListener(e -> {
            if (inspectorItem.isSelected()) {
                focusAuxiliaryPanel(inspectorPanel);
            } else {
                hideRightPanel();
            }
        });
        view.add(inspectorItem);
        view.addSeparator();
        JMenuItem focusEditor = new JMenuItem(new AbstractAction("聚焦编辑器") {
            @Override
            public void actionPerformed(ActionEvent e) {
                focusEditorArea();
            }
        });
        focusEditor.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_L, InputEvent.CTRL_DOWN_MASK));
        view.add(focusEditor);
        JMenuItem cycleFocus = new JMenuItem(new AbstractAction("循环焦点") {
            @Override
            public void actionPerformed(ActionEvent e) {
                focusNextMajorArea();
            }
        });
        cycleFocus.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F6, 0));
        view.add(cycleFocus);
        view.addSeparator();
        view.add(new JMenuItem(new AbstractAction("重置布局") {
            @Override
            public void actionPerformed(ActionEvent e) {
                resetLayout();
            }
        }));

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
                if (panel != null) {
                    if (panel.isTemporary()) {
                        saveTemporaryPanel(panel);
                    } else {
                        panel.saveNow();
                    }
                }
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
        file.add(new JMenuItem(new AbstractAction("垃圾站") {
            @Override
            public void actionPerformed(ActionEvent e) {
                openTrashBin();
            }
        }));
        JMenuItem closeCurrent = new JMenuItem(new AbstractAction("关闭当前标签") {
            @Override
            public void actionPerformed(ActionEvent e) {
                closeCurrentContainer();
            }
        });
        closeCurrent.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_W, InputEvent.CTRL_DOWN_MASK));
        file.add(closeCurrent);
        file.addSeparator();
        file.add(new JMenuItem(new AbstractAction("退出") {
            @Override
            public void actionPerformed(ActionEvent e) {
                dispose();
                System.exit(0);
            }
        }));

        JMenuItem runCurrent = new JMenuItem(new AbstractAction("执行当前") {
            @Override
            public void actionPerformed(ActionEvent e) {
                executeCurrentSql();
            }
        });
        runCurrent.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, InputEvent.CTRL_DOWN_MASK));
        run.add(runCurrent);

        JMenuItem stopCurrent = new JMenuItem(new AbstractAction("停止/取消执行") {
            @Override
            public void actionPerformed(ActionEvent e) {
                stopIfRunning();
            }
        });
        stopCurrent.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0));
        run.add(stopCurrent);

        JMenuItem focusEditorRun = new JMenuItem(new AbstractAction("切换 Tab (下一个)") {
            @Override
            public void actionPerformed(ActionEvent e) {
                navigateEditors(true);
            }
        });
        focusEditorRun.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_TAB, InputEvent.CTRL_DOWN_MASK));
        run.add(focusEditorRun);
        JMenuItem focusEditorRunPrev = new JMenuItem(new AbstractAction("切换 Tab (上一个)") {
            @Override
            public void actionPerformed(ActionEvent e) {
                navigateEditors(false);
            }
        });
        focusEditorRunPrev.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_TAB, InputEvent.CTRL_DOWN_MASK | InputEvent.SHIFT_DOWN_MASK));
        run.add(focusEditorRunPrev);

        JMenuItem editorSettings = new JMenuItem(new AbstractAction("SQL 编辑器选项…") {
            @Override
            public void actionPerformed(ActionEvent e) {
                openEditorSettings();
            }
        });
        editorSettings.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_COMMA, InputEvent.CTRL_DOWN_MASK));
        edit.add(editorSettings);

        tools.add(new JMenuItem(new AbstractAction("刷新元数据") {
            @Override
            public void actionPerformed(ActionEvent e) {
                OperationLog.log("用户手动刷新元数据");
                metadataService.refreshMetadataAsync(MainFrame.this::handleMetadataRefreshResult);
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("Refresh Metadata (Large)") {
            @Override
            public void actionPerformed(ActionEvent e) {
                OperationLog.log("开发者自测：大规模元数据刷新");
                metadataService.refreshMetadataAsync(true, result -> {
                    handleMetadataRefreshResult(result);
                    SwingUtilities.invokeLater(() -> {
                        String msg;
                        int messageType;
                        if (result != null && result.success()) {
                            msg = String.format("刷新成功%n对象: %d%n列: %d%n批次: %d%n耗时: %d ms",
                                    result.totalObjects(),
                                    result.totalColumns(),
                                    result.batches(),
                                    result.durationMillis());
                            messageType = JOptionPane.INFORMATION_MESSAGE;
                        } else {
                            msg = "刷新失败: " + (result == null ? "未知" : result.message());
                            messageType = JOptionPane.ERROR_MESSAGE;
                        }
                        JOptionPane.showMessageDialog(MainFrame.this, msg, "Refresh Metadata (Large)", messageType);
                    });
                });
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("取消元数据刷新") {
            @Override
            public void actionPerformed(ActionEvent e) {
                OperationLog.log("用户请求取消元数据刷新");
                metadataService.cancelRefresh();
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
                    OperationLog.log("确认重置元数据并重新拉取");
                    metadataService.resetMetadataAsync(result -> {
                        handleMetadataRefreshResult(result);
                        JOptionPane.showMessageDialog(MainFrame.this,
                                "已重置并重新获取元数据。",
                                "完成",
                                JOptionPane.INFORMATION_MESSAGE);
                    });
                }
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("开启 Debug 模式") {
            @Override
            public void actionPerformed(ActionEvent e) {
                enableDebugMode();
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("备份本地数据库") {
            @Override
            public void actionPerformed(ActionEvent e) {
                backupLocalDatabase();
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("恢复本地数据库") {
            @Override
            public void actionPerformed(ActionEvent e) {
                restoreLocalDatabase();
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("SQL片段库 (Alt+W)") {
            @Override
            public void actionPerformed(ActionEvent e) {
                getQuickSqlSnippetDialog().toggleVisible();
            }
        }));
        tools.add(new JMenuItem(new AbstractAction("执行历史 (Alt+Q)") {
            @Override
            public void actionPerformed(ActionEvent e) {
                getExecutionHistoryDialog().toggleVisible();
            }
        }));

        tools.add(new JMenuItem(new AbstractAction("定时备份设置") {
            @Override
            public void actionPerformed(ActionEvent e) {
                configureScheduledBackup();
            }
        }));

        JMenuItem pasteImport = new JMenuItem(new AbstractAction("粘贴导入") {
            @Override
            public void actionPerformed(ActionEvent e) {
                ExcelPasteImportDialog dialog = new ExcelPasteImportDialog(MainFrame.this, sqlExecutionService);
                dialog.setVisible(true);
            }
        });
        pasteImport.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_V, InputEvent.CTRL_DOWN_MASK | InputEvent.SHIFT_DOWN_MASK));
        tools.add(pasteImport);

        tools.add(new JMenuItem(new AbstractAction("文本解析") {
            @Override
            public void actionPerformed(ActionEvent e) {
                TextParseDialog dialog = new TextParseDialog(MainFrame.this);
                dialog.setVisible(true);
            }
        }));

        view.add(new JMenuItem(new AbstractAction("切换左侧面板") {
            @Override
            public void actionPerformed(ActionEvent e) {
                toggleLeftPanel();
            }
        }));
        view.add(new JMenuItem(new AbstractAction("切换底部面板") {
            @Override
            public void actionPerformed(ActionEvent e) {
                toggleBottomPanel();
            }
        }));
        view.add(new JMenuItem(new AbstractAction("切换右侧面板") {
            @Override
            public void actionPerformed(ActionEvent e) {
                toggleRightPanel();
            }
        }));

        JMenu logDockMenu = new JMenu("日志停靠位置");
        ButtonGroup logGroup = new ButtonGroup();
        DockPosition currentDock = layoutState.getLogDockPosition(DockPosition.BOTTOM);
        addDockMenuItem(logDockMenu, logGroup, "底部", DockPosition.BOTTOM, currentDock == DockPosition.BOTTOM);
        addDockMenuItem(logDockMenu, logGroup, "右侧", DockPosition.RIGHT, currentDock == DockPosition.RIGHT);
        addDockMenuItem(logDockMenu, logGroup, "浮动窗口", DockPosition.FLOATING, currentDock == DockPosition.FLOATING);
        view.add(logDockMenu);

        view.add(new JMenuItem(new AbstractAction("重置布局") {
            @Override
            public void actionPerformed(ActionEvent e) {
                resetLayout();
            }
        }));

        view.add(new JMenuItem(new AbstractAction("对象浏览器") {
            @Override
            public void actionPerformed(ActionEvent e) {
                showObjectBrowser();
            }
        }));

        view.add(new JMenuItem(new AbstractAction("异步任务列表") {
            @Override
            public void actionPerformed(ActionEvent e) {
                AsyncJobListDialog dialog = new AsyncJobListDialog(MainFrame.this, sqlExecutionService);
                dialog.setVisible(true);
            }
        }));

        JMenu themeMenu = new JMenu("主题");
        ButtonGroup themeGroup = new ButtonGroup();
        for (ThemeOption option : themeManager.getOptions()) {
            boolean selected = currentTheme != null && currentTheme.getId().equals(option.getId());
            JRadioButtonMenuItem item = new JRadioButtonMenuItem(option.getName(), selected);
            item.addActionListener(e -> switchTheme(option));
            themeGroup.add(item);
            themeMenu.add(item);
        }
        view.add(themeMenu);
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

        view.add(new JMenuItem(new AbstractAction("平铺窗口") {
            @Override
            public void actionPerformed(ActionEvent e) {
                tileFrames();
            }
        }));

        help.add(new JMenuItem(new AbstractAction("使用说明") {
            @Override
            public void actionPerformed(ActionEvent e) {
                showUsageGuide();
            }
        }));
        JMenuItem shortcutHelp = new JMenuItem(new AbstractAction("快捷键列表") {
            @Override
            public void actionPerformed(ActionEvent e) {
                showShortcutDialog();
            }
        });
        shortcutHelp.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_SLASH, InputEvent.CTRL_DOWN_MASK | InputEvent.SHIFT_DOWN_MASK));
        help.add(shortcutHelp);

        menuBar.add(file);
        menuBar.add(edit);
        menuBar.add(run);
        menuBar.add(view);
        menuBar.add(tools);
        menuBar.add(help);
        setJMenuBar(menuBar);
    }

    private void buildToolbar() {
        JToolBar toolBar = new JToolBar();
        toolBar.setFloatable(false);
        toolBar.setBorder(new EmptyBorder(4, 8, 4, 8));
        executeButton = new JButton(IconFactory.createRunIcon(true));
        executeButton.setDisabledIcon(IconFactory.createRunIcon(false));
        executeButton.setToolTipText("执行 (Ctrl+Enter)");
        stopButton = new JButton(IconFactory.createStopIcon(true));
        stopButton.setDisabledIcon(IconFactory.createStopIcon(false));
        stopButton.setToolTipText("停止执行");
        stopButton.setEnabled(false);
        executeButton.addActionListener(e -> executeCurrentSql());
        stopButton.addActionListener(e -> stopCurrentExecution());
        styleToolbarButton(executeButton);
        styleToolbarButton(stopButton);
        toolBar.add(executeButton);
        toolBar.add(stopButton);
        JButton newTabButton = new JButton("新建标签");
        newTabButton.setToolTipText("新建笔记标签");
        newTabButton.addActionListener(e -> createNote(DatabaseType.POSTGRESQL));
        styleToolbarButton(newTabButton);
        toolBar.add(newTabButton);
        JButton toggleLeftButton = new JButton("切换导航");
        toggleLeftButton.setToolTipText("显示/隐藏对象浏览器");
        toggleLeftButton.addActionListener(e -> toggleLeftPanel());
        styleToolbarButton(toggleLeftButton);
        toolBar.add(toggleLeftButton);
        add(toolBar, BorderLayout.NORTH);
    }

    private void buildContent() {
        centerPanel.setMinimumSize(new Dimension(200, 240));
        centerPanel.add(desktopPane, "window");
        centerPanel.add(tabbedPane, "panel");
        centerPanel.setBorder(new EmptyBorder(4, 8, 4, 8));
        sharedResultsScroll = new JScrollPane(sharedResultView.wrapper);
        sharedResultsScroll.setBorder(BorderFactory.createEmptyBorder());
        sharedResultsWrapper.add(sharedResultsScroll, BorderLayout.CENTER);
        sharedResultsWrapper.setMinimumSize(new Dimension(100, 180));
        desktopPane.addPropertyChangeListener("selectedFrame", evt -> {
            syncActivePanelWithSelection();
            updateExecutionButtons();
        });
        tabbedPane.addChangeListener(e -> {
            syncActivePanelWithSelection();
            updateExecutionButtons();
        });

        resultTabs = new JTabbedPane();
        resultTabs.addTab("结果", sharedResultsWrapper);
        messageArea = new JTextArea();
        messageArea.setEditable(false);
        messageArea.setLineWrap(true);
        messageArea.setWrapStyleWord(true);
        JScrollPane messageScroll = new JScrollPane(messageArea);
        resultTabs.addTab("消息", messageScroll);
        bottomTabbedPane = resultTabs;
        bottomTabbedPane.setMinimumSize(new Dimension(200, 160));

        auxiliaryTabs = new JTabbedPane();
        logPanel = new LogPanel();
        historyPanel = createHistoryShortcutPanel();
        inspectorPanel = createInspectorPlaceholder();
        auxiliaryTabs.addTab("日志", logPanel);
        auxiliaryTabs.addTab("历史", historyPanel);
        auxiliaryTabs.addTab("设置", inspectorPanel);
        auxiliaryPanel = new JPanel(new BorderLayout());
        auxiliaryPanel.add(auxiliaryTabs, BorderLayout.CENTER);
        auxiliaryPanel.setPreferredSize(new Dimension(320, 400));
        auxiliaryPanel.setVisible(false);

        leftPanel = buildObjectBrowserPanel();
        centerRightSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, centerPanel, bottomTabbedPane);
        centerRightSplit.setResizeWeight(0.72);
        centerRightSplit.setDividerSize(8);
        centerRightSplit.setContinuousLayout(true);
        centerRightSplit.setBorder(BorderFactory.createEmptyBorder());

        leftSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPanel, centerRightSplit);
        leftSplit.setResizeWeight(0.2);
        leftSplit.setDividerSize(8);
        leftSplit.setContinuousLayout(true);
        leftSplit.setBorder(BorderFactory.createEmptyBorder());

        add(leftSplit, BorderLayout.CENTER);
        add(auxiliaryPanel, BorderLayout.EAST);

        SwingUtilities.invokeLater(this::applyInitialLayoutState);
        centerLayout.show(centerPanel, "window");
        restoreSession();
    }

    private void applyInitialLayoutState() {
        DockPosition dock = layoutState.getLogDockPosition(DockPosition.BOTTOM);
        moveLogTo(dock);
        if (!leftVisible) {
            hideLeftPanel();
        } else {
            showLeftPanel();
        }
        if (!bottomVisible) {
            hideBottomPanel();
        } else {
            showBottomPanel();
        }
        applyEditorMaximizedState();
        SwingUtilities.invokeLater(() -> {
            int left = layoutState.getLeftDivider(leftSplit.getDividerLocation());
            int right = layoutState.getRightDivider(centerRightSplit.getDividerLocation());
            int bottom = layoutState.getBottomDivider(centerRightSplit.getDividerLocation());
            if (left > 0) leftSplit.setDividerLocation(left);
            if (right > 0) centerRightSplit.setDividerLocation(right);
            if (bottom > 0) centerRightSplit.setDividerLocation(bottom);
        });
    }

    private JPanel buildObjectBrowserPanel() {
        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBorder(new EmptyBorder(8, 8, 8, 8));
        navigationBrowserPanel = new ObjectBrowserPanel(metadataService, pgRoutineService,
                new ObjectBrowserPanel.RoutineActionHandler() {
                    @Override
                    public void openRoutine(Component source, RoutineInfo info, boolean editable) {
                        openRoutineEditor(info, editable, source);
                    }

                    @Override
                    public void runRoutine(Component source, RoutineInfo info) {
                        runRoutineWithDialog(info, source);
                    }
                });
        navigationTree = navigationBrowserPanel.getTree();

        JButton openDialog = new JButton("弹出");
        openDialog.addActionListener(e -> {
            objectBrowserDialog = new ObjectBrowserDialog(MainFrame.this, metadataService, pgRoutineService,
                    new ObjectBrowserDialog.RoutineActionHandler() {
                        @Override
                        public void openRoutine(Component source, RoutineInfo info, boolean editable) {
                            openRoutineEditor(info, editable, source);
                        }

                        @Override
                        public void runRoutine(Component source, RoutineInfo info) {
                            runRoutineWithDialog(info, source);
                        }
                    });
            objectBrowserDialog.setVisible(true);
        });
        navigationBrowserPanel.setHeaderTrailing(openDialog);
        panel.add(navigationBrowserPanel, BorderLayout.CENTER);

        return panel;
    }

    private JComponent createInspectorPlaceholder() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(8, 8, 8, 8));
        JLabel label = new JLabel("右侧 Inspector 可显示字段说明或执行提示。");
        label.setHorizontalAlignment(SwingConstants.LEFT);
        JTextArea desc = new JTextArea("在此区域可以扩展显示元数据详情、执行参数或任务说明。当前版本提供简要提示。");
        desc.setEditable(false);
        desc.setLineWrap(true);
        desc.setWrapStyleWord(true);
        desc.setOpaque(false);
        panel.add(label, BorderLayout.NORTH);
        panel.add(new JScrollPane(desc), BorderLayout.CENTER);
        return panel;
    }

    private JComponent createJobsShortcutPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        JButton openJobs = new JButton("打开异步任务列表");
        openJobs.addActionListener(e -> {
            AsyncJobListDialog dialog = new AsyncJobListDialog(MainFrame.this, sqlExecutionService);
            dialog.setVisible(true);
        });
        panel.add(new JLabel("任务列表将展示正在运行或已完成的后台任务。"), BorderLayout.NORTH);
        panel.add(openJobs, BorderLayout.CENTER);
        return panel;
    }

    private JComponent createHistoryShortcutPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        JButton openHistory = new JButton("打开执行历史");
        openHistory.addActionListener(e -> getExecutionHistoryDialog().toggleVisible());
        panel.add(new JLabel("查看历史可以帮助回溯最近执行的 SQL。"), BorderLayout.NORTH);
        panel.add(openHistory, BorderLayout.CENTER);
        return panel;
    }

    private void toggleLeftPanel() {
        if (leftVisible) {
            hideLeftPanel();
        } else {
            showLeftPanel();
        }
    }

    private void showLeftPanel() {
        leftVisible = true;
        layoutState.setLeftVisible(true);
        leftPanel.setVisible(true);
        int divider = layoutState.getLeftDivider(lastLeftDividerLocation > 0 ? lastLeftDividerLocation : (int) (getWidth() * 0.2));
        SwingUtilities.invokeLater(() -> leftSplit.setDividerLocation(Math.max(160, divider)));
    }

    private void hideLeftPanel() {
        if (leftSplit != null) {
            lastLeftDividerLocation = leftSplit.getDividerLocation();
            SwingUtilities.invokeLater(() -> leftSplit.setDividerLocation(0));
        }
        leftVisible = false;
        layoutState.setLeftVisible(false);
        leftPanel.setVisible(false);
    }

    private void toggleRightPanel() {
        if (rightVisible) {
            hideRightPanel();
        } else {
            showRightPanel();
        }
    }

    private void showRightPanel() {
        rightVisible = true;
        layoutState.setRightVisible(true);
        if (auxiliaryPanel != null) {
            auxiliaryPanel.setVisible(true);
            auxiliaryPanel.revalidate();
            auxiliaryPanel.repaint();
        }
    }

    private void hideRightPanel() {
        rightVisible = false;
        layoutState.setRightVisible(false);
        if (auxiliaryPanel != null) {
            auxiliaryPanel.setVisible(false);
            auxiliaryPanel.revalidate();
            auxiliaryPanel.repaint();
        }
    }

    private void focusAuxiliaryPanel(Component component) {
        if (auxiliaryPanel == null || auxiliaryTabs == null || component == null) {
            return;
        }
        if (auxiliaryTabs.indexOfComponent(component) >= 0) {
            auxiliaryPanel.setVisible(true);
            rightVisible = true;
            layoutState.setRightVisible(true);
            auxiliaryTabs.setSelectedComponent(component);
            auxiliaryPanel.revalidate();
            auxiliaryPanel.repaint();
        }
    }

    private void toggleBottomPanel() {
        if (bottomVisible) {
            hideBottomPanel();
        } else {
            showBottomPanel();
        }
    }

    private void showBottomPanel() {
        bottomVisible = true;
        layoutState.setBottomVisible(true);
        bottomTabbedPane.setVisible(true);
        int divider = layoutState.getBottomDivider(lastBottomDividerLocation > 0 ? lastBottomDividerLocation : (int) (getHeight() * 0.72));
        SwingUtilities.invokeLater(() -> centerRightSplit.setDividerLocation(divider));
    }

    private void hideBottomPanel() {
        if (centerRightSplit != null) {
            lastBottomDividerLocation = centerRightSplit.getDividerLocation();
            SwingUtilities.invokeLater(() -> centerRightSplit.setDividerLocation(centerRightSplit.getHeight()));
        }
        bottomVisible = false;
        layoutState.setBottomVisible(false);
        bottomTabbedPane.setVisible(false);
    }

    private void toggleEditorMaximize() {
        if (!editorMaximized) {
            savedLeftVisibleForMax = leftVisible;
            savedRightVisibleForMax = rightVisible;
            savedBottomVisibleForMax = bottomVisible;
            hideLeftPanel();
            hideRightPanel();
            hideBottomPanel();
            editorMaximized = true;
        } else {
            if (savedLeftVisibleForMax) showLeftPanel();
            if (savedRightVisibleForMax) showRightPanel();
            if (savedBottomVisibleForMax) showBottomPanel();
            editorMaximized = false;
        }
        layoutState.setEditorMaximized(editorMaximized);
    }

    private void applyEditorMaximizedState() {
        if (editorMaximized) {
            savedLeftVisibleForMax = leftVisible;
            savedRightVisibleForMax = rightVisible;
            savedBottomVisibleForMax = bottomVisible;
            hideLeftPanel();
            hideRightPanel();
            hideBottomPanel();
        }
    }

    private void moveLogTo(DockPosition position) {
        if (dockManager == null || logPanel == null) return;
        if (dockManager.getCurrentPosition(logPanel) == DockPosition.FLOATING) {
            floatingLogBounds = dockManager.captureFloatingBounds(logPanel);
            layoutState.saveLogFloatingBounds(floatingLogBounds);
        }
        dockManager.dock(logPanel, position, "日志", floatingLogBounds);
        layoutState.setLogDockPosition(position);
        if (position == DockPosition.BOTTOM) {
            showBottomPanel();
            bottomTabbedPane.setSelectedComponent(logPanel);
        } else if (position == DockPosition.RIGHT) {
            showRightPanel();
            rightTabbedPane.setSelectedComponent(logPanel);
        }
    }

    private void focusLogPanel() {
        DockPosition position = dockManager != null ? dockManager.getCurrentPosition(logPanel) : DockPosition.BOTTOM;
        moveLogTo(position);
        if (position == DockPosition.FLOATING) {
            dockManager.undockToDialog(logPanel, "操作日志", floatingLogBounds);
        }
    }

    private void persistLayoutState() {
        if (leftSplit != null) {
            layoutState.setLeftDivider(leftSplit.getDividerLocation());
        }
        if (centerRightSplit != null) {
            layoutState.setRightDivider(centerRightSplit.getDividerLocation());
        }
        if (centerRightSplit != null) {
            layoutState.setBottomDivider(centerRightSplit.getDividerLocation());
        }
        layoutState.saveWindowBounds(getBounds());
        layoutState.setLeftVisible(leftVisible);
        layoutState.setRightVisible(rightVisible);
        layoutState.setBottomVisible(bottomVisible);
        layoutState.setEditorMaximized(editorMaximized);
        if (dockManager != null) {
            layoutState.setLogDockPosition(dockManager.getCurrentPosition(logPanel));
            Rectangle bounds = dockManager.captureFloatingBounds(logPanel);
            if (bounds != null) {
                layoutState.saveLogFloatingBounds(bounds);
            }
        }
    }

    private void addDockMenuItem(JMenu menu, ButtonGroup group, String label, DockPosition position, boolean selected) {
        JRadioButtonMenuItem item = new JRadioButtonMenuItem(label, selected);
        item.addActionListener(e -> moveLogTo(position));
        group.add(item);
        menu.add(item);
    }

    private void resetLayout() {
        layoutState.reset();
        leftVisible = true;
        rightVisible = false;
        bottomVisible = true;
        editorMaximized = false;
        applyInitialLayoutState();
    }

    private void restoreSession() {
        java.util.List<Long> openIds = appStateRepository.loadOpenNotes();
        if (!openIds.isEmpty()) {
            noteRepository.listByIds(openIds).forEach(this::openNoteInCurrentMode);
            focusByNoteId(openIds.get(openIds.size() - 1));
        }
        if (panelCache.isEmpty()) {
            createNote(DatabaseType.POSTGRESQL);
        }
    }

    private void buildStatusBar() {
        JPanel status = new JPanel(new BorderLayout());
        status.setBorder(new EmptyBorder(4, 8, 4, 8));
        JPanel left = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        left.add(statusLabel);
        left.add(autosaveLabel);
        left.add(taskLabel);
        left.add(suggestionStateLabel);
        left.add(metadataLabel);
        status.add(left, BorderLayout.WEST);
        statusBar = status;
        add(status, BorderLayout.SOUTH);
    }

    private void createNote(DatabaseType type) {
        String title = UntitledNoteNamer.nextTitle(t -> noteRepository.titleExists(t, null));
        Note note = createTemporaryNote(type, title, "");
        OperationLog.log("新建临时笔记: " + title + " [" + type + "]");
        openNoteInCurrentMode(note);
    }

    private EditorTabPanel getOrCreatePanel(Note note) {
        return panelCache.computeIfAbsent(note.getId(), id -> {
            NotePersistenceStrategy strategy = note.isTemporary()
                    ? new TemporaryNotePersistenceStrategy()
                    : new PersistentNotePersistenceStrategy();
            EditorTabPanel p = new EditorTabPanel(
                    noteRepository,
                    metadataService,
                    this::updateAutosaveTime,
                    this::updateTaskCount,
                    newTitle -> updateTitleForPanel(newTitle, id),
                    note,
                    convertFullWidth,
                    resolveStyleForNote(note),
                    this::onPanelFocused,
                    this::openNoteByTitle,
                    defaultPageSize,
                    strategy
            );
            p.setExecuteHandler(() -> executeCurrentSql(true));
            p.setSuggestionEnabled(suggestionEnabled);
            refreshPanelStyleMenu(p);
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

    private void openNoteWithNavigation(Note note, SearchNavigation navigation) {
        openNoteInCurrentMode(note);
        if (note == null || navigation == null) {
            return;
        }
        EditorTabPanel panel = getOrCreatePanel(note);
        panel.revealMatch(navigation.offset(), navigation.keyword());
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
                } catch (Exception ignored) {
                }
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
        JInternalFrame frame = new JInternalFrame(getPanelDisplayTitle(panel), true, true, true, true);
        // 为子窗体设置持久化的随机绘制图标
        frame.setFrameIcon(getOrCreateNoteIcon(panel.getNote()));
        frame.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
        frame.setSize(600, 400);
        frame.setLocation(20 * desktopPane.getAllFrames().length,
                20 * desktopPane.getAllFrames().length);
        frame.setVisible(true);
        frame.add(panel, BorderLayout.CENTER);
        frame.setToolTipText(getPanelTooltip(panel));
        installRenameHandler(frame, panel);
        frame.addInternalFrameListener(new InternalFrameAdapter() {
            @Override
            public void internalFrameClosing(InternalFrameEvent e) {
                if (!confirmTemporaryClose(panel)) {
                    return;
                }
                unregisterRoutinePanel(panel);
                panelCache.remove(panel.getNote().getId());
                persistOpenFrames();
                frame.dispose();
            }
        });
        desktopPane.add(frame);
        try {
            frame.setSelected(true);
        } catch (java.beans.PropertyVetoException ignored) {
        }
        persistOpenFrames();
        onPanelFocused(panel);
        updateExecutionButtons();
    }

    private void addTab(EditorTabPanel panel) {
        detachFromParent(panel);
        tabbedPane.addTab(getPanelDisplayTitle(panel), panel);
        int idx = tabbedPane.indexOfComponent(panel);
        // 面板模式下标签使用与子窗体相同的图标
        tabbedPane.setIconAt(idx, getOrCreateNoteIcon(panel.getNote()));
        tabbedPane.setToolTipTextAt(idx, getPanelTooltip(panel));
        tabbedPane.setSelectedIndex(idx);
        persistOpenFrames();
        onPanelFocused(panel);
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
                        String newTitle = JOptionPane.showInputDialog(
                                MainFrame.this, "重命名窗口", frame.getTitle());
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
        if (activePanel != null && activePanel.isShowing()) {
            return activePanel;
        }
        return selectedPanel();
    }

    private EditorTabPanel selectedPanel() {
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

    private void syncActivePanelWithSelection() {
        EditorTabPanel selected = selectedPanel();
        if (selected != null && activePanel != null && activePanel != selected) {
            activePanel.hideSuggestionPopup("tabChanged");
        }
        if (selected != null) {
            activePanel = selected;
        }
    }

    private void closeCurrentContainer() {
        if (windowMode) {
            JInternalFrame frame = desktopPane.getSelectedFrame();
            if (frame != null) {
                EditorTabPanel panel = extractPanelFromFrame(frame);
                if (panel != null) {
                    if (!confirmTemporaryClose(panel)) {
                        return;
                    }
                    unregisterRoutinePanel(panel);
                    panelCache.remove(panel.getNote().getId());
                }
                frame.dispose();
            }
        } else {
            int idx = tabbedPane.getSelectedIndex();
            if (idx >= 0) {
                Component comp = tabbedPane.getComponentAt(idx);
                EditorTabPanel panel = extractPanel(comp);
                if (panel != null) {
                    if (!confirmTemporaryClose(panel)) {
                        return;
                    }
                    unregisterRoutinePanel(panel);
                    panelCache.remove(panel.getNote().getId());
                }
                tabbedPane.removeTabAt(idx);
            }
        }
        persistOpenFrames();
        syncActivePanelWithSelection();
        updateExecutionButtons();
    }

    private boolean confirmTemporaryClose(EditorTabPanel panel) {
        if (panel == null || !panel.isTemporary()) {
            return true;
        }
        if (!panel.isDirty()) {
            return true;
        }
        Object[] options = {"保存为正式笔记", "不保存", "取消"};
        int choice = JOptionPane.showOptionDialog(
                this,
                "临时笔记内容已修改，是否保存为正式笔记？",
                "关闭临时笔记",
                JOptionPane.DEFAULT_OPTION,
                JOptionPane.WARNING_MESSAGE,
                null,
                options,
                options[0]
        );
        if (choice == 0) {
            return saveTemporaryPanel(panel);
        }
        if (choice == 1) {
            return true;
        }
        return false;
    }

    private boolean saveTemporaryPanel(EditorTabPanel panel) {
        if (panel == null) {
            return false;
        }
        while (true) {
            String suggestion = panel.getNote().getTitle();
            String title = JOptionPane.showInputDialog(this, "保存为正式笔记", suggestion);
            if (title == null) {
                return false;
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
            try {
                Note created = noteRepository.create(title, panel.getNote().getDatabaseType());
                noteRepository.updateContent(created, panel.getSqlText());
                try {
                    LinkResolver.resolveAndPersistLinks(noteRepository, created, panel.getSqlText());
                } catch (Exception ex) {
                    OperationLog.log("解析/持久化链接失败: " + ex.getMessage());
                }
                convertTemporaryPanel(panel, created);
                return true;
            } catch (Exception ex) {
                JOptionPane.showMessageDialog(this, "保存失败: " + ex.getMessage());
                return false;
            }
        }
    }

    private void convertTemporaryPanel(EditorTabPanel panel, Note created) {
        long oldId = panel.getNote().getId();
        created.setTemporary(false);
        panel.replaceNote(created, new PersistentNotePersistenceStrategy(),
                newTitle -> updateTitleForPanel(newTitle, created.getId()));
        panelCache.remove(oldId);
        panelCache.put(created.getId(), panel);
        if (runningExecutions.containsKey(oldId)) {
            runningExecutions.put(created.getId(), runningExecutions.remove(oldId));
        }
        if (resultTabCounters.containsKey(oldId)) {
            resultTabCounters.put(created.getId(), resultTabCounters.remove(oldId));
        }
        Icon icon = getOrCreateNoteIcon(created);
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel framePanel = extractPanelFromFrame(frame);
            if (framePanel == panel) {
                frame.setFrameIcon(icon);
                frame.setTitle(getPanelDisplayTitle(panel));
                break;
            }
        }
        int idx = tabbedPane.indexOfComponent(panel);
        if (idx >= 0) {
            tabbedPane.setIconAt(idx, icon);
        }
        refreshPanelTitle(panel);
        persistOpenFrames();
    }

    private void navigateEditors(boolean forward) {
        if (windowMode) {
            JInternalFrame[] frames = desktopPane.getAllFrames();
            if (frames.length == 0) {
                return;
            }
            java.util.List<JInternalFrame> list = java.util.Arrays.asList(frames);
            JInternalFrame current = desktopPane.getSelectedFrame();
            int idx = list.indexOf(current);
            int next = (idx < 0 ? 0 : idx + (forward ? 1 : -1));
            if (next < 0) next = list.size() - 1;
            if (next >= list.size()) next = 0;
            JInternalFrame target = list.get(next);
            try {
                target.setIcon(false);
                target.setSelected(true);
                target.toFront();
            } catch (Exception ignored) {
            }
        } else {
            int count = tabbedPane.getTabCount();
            if (count == 0) return;
            int idx = tabbedPane.getSelectedIndex();
            int next = (idx < 0 ? 0 : idx + (forward ? 1 : -1));
            if (next < 0) next = count - 1;
            if (next >= count) next = 0;
            tabbedPane.setSelectedIndex(next);
        }
        syncActivePanelWithSelection();
    }

    private void focusEditorArea() {
        EditorTabPanel panel = getCurrentPanel();
        if (panel != null) {
            panel.focusEditorArea();
        }
    }

    private void focusNextMajorArea() {
        java.util.List<Runnable> targets = new java.util.ArrayList<>();
        if (navigationTree != null && leftPanel != null && leftPanel.isShowing()) {
            targets.add(() -> {
                showLeftPanel();
                navigationTree.requestFocusInWindow();
                if (navigationTree.getRowCount() > 0 && navigationTree.getSelectionCount() == 0) {
                    navigationTree.setSelectionRow(0);
                }
            });
        }
        EditorTabPanel panel = getCurrentPanel();
        if (panel != null) {
            targets.add(panel::focusEditorArea);
            targets.add(() -> {
                showBottomPanel();
                panel.focusResultArea();
            });
        } else {
            targets.add(this::focusSharedResults);
        }
        if (targets.isEmpty()) {
            return;
        }
        focusCycleIndex = (focusCycleIndex + 1) % targets.size();
        targets.get(focusCycleIndex).run();
    }

    private void focusSharedResults() {
        if (sharedResultView == null) {
            return;
        }
        Component comp = sharedResultView.tabs.getSelectedComponent();
        if (comp instanceof QueryResultPanel qp) {
            qp.focusTable();
        } else {
            sharedResultView.tabs.requestFocusInWindow();
        }
    }

    private void onPanelFocused(EditorTabPanel panel) {
        if (panel == null) return;
        activePanel = panel;
        selectContainerForPanel(panel);
        updateExecutionButtons();
    }

    private void selectContainerForPanel(EditorTabPanel panel) {
        if (panel == null) return;
        if (windowMode) {
            for (JInternalFrame frame : desktopPane.getAllFrames()) {
                if (extractPanelFromFrame(frame) == panel) {
                    try {
                        frame.setSelected(true);
                    } catch (java.beans.PropertyVetoException ignored) {
                    }
                    frame.toFront();
                    break;
                }
            }
        } else {
            int idx = tabbedPane.indexOfComponent(panel);
            if (idx >= 0) {
                tabbedPane.setSelectedIndex(idx);
            }
        }
    }

    private void focusByNoteId(Long noteId) {
        if (noteId == null) return;
        for (EditorTabPanel panel : panelCache.values()) {
            if (panel.getNote().getId() == noteId) {
                onPanelFocused(panel);
                break;
            }
        }
    }

    private void saveAll() {
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel panel = extractPanelFromFrame(frame);
            if (panel != null) {
                if (panel.isTemporary()) {
                    saveTemporaryPanel(panel);
                } else {
                    panel.saveNow();
                }
            }
        }
        for (int i = 0; i < tabbedPane.getTabCount(); i++) {
            Component comp = tabbedPane.getComponentAt(i);
            EditorTabPanel panel = extractPanel(comp);
            if (panel != null) {
                if (panel.isTemporary()) {
                    saveTemporaryPanel(panel);
                } else {
                    panel.saveNow();
                }
            }
        }
        persistOpenFrames();
    }

    private void executeCurrentSql() {
        executeCurrentSql(true);
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

        java.util.List<String> statements = panel.getExecutableStatements(blockMode).stream()
                .map(LinkResolver::stripLinkTags)
                .filter(s -> s != null && !s.isBlank())
                .toList();
        if (statements.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请选择要执行的 SQL 语句");
            return;
        }

        sqlHistoryRepository.recordExecution(String.join(";\n", statements), panel.getNote().getDatabaseType());

        // 清理旧结果
        if (windowMode) {
            panel.clearLocalResults();
        } else {
            SharedResultView target = ensureSharedView();
            target.clear();
            collapseSharedResults();
        }

        // 重置结果计数 & 状态
        resultTabCounters.put(noteId, new java.util.concurrent.atomic.AtomicInteger(1));
        statusLabel.setText("执行中...");
        panel.setExecutionRunning(true);
        OperationLog.log("开始执行 SQL，共 " + statements.size() + " 条 | " + panel.getNote().getTitle());

        // 按笔记 ID 记录运行中的 Future
        runningExecutions.putIfAbsent(noteId, new CopyOnWriteArrayList<>());
        List<RunningJobHandle> execList = runningExecutions.get(noteId);

        String dbUser = panel.getSelectedDbUser();
        int pageSize = panel.getPreferredPageSize();
        OperationLog.log("本次查询使用 pageSize=" + pageSize);

        // 串行执行每一条 SQL
        java.util.concurrent.CompletableFuture<Void> chain =
                java.util.concurrent.CompletableFuture.completedFuture(null);

        for (String stmt : statements) {
            final String sqlStmt = stmt;
            chain = chain.thenCompose(v -> {
                QueryResultPanel pendingPanel = addPendingResultPanel(noteId, panel, sqlStmt);
                RunningJobHandle handle = new RunningJobHandle();
                handle.panel = pendingPanel;
                handle.sql = sqlStmt;

                CompletableFuture<Void> future = sqlExecutionService.execute(
                        sqlStmt,
                        dbUser,
                        pageSize,
                        res -> {
                            SqlExecResult normalized = columnOrderDecider.reorder(res,
                                    refreshed -> SwingUtilities.invokeLater(() -> renderResult(noteId, panel, refreshed, pendingPanel)));
                            SwingUtilities.invokeLater(() -> renderResult(noteId, panel, normalized, pendingPanel));
                        },
                        ex -> SwingUtilities.invokeLater(() -> renderError(noteId, panel, sqlStmt, ex, pendingPanel)),
                        status -> SwingUtilities.invokeLater(() -> handleStatusUpdate(handle, status, panel))
                );

                handle.future = future;
                execList.add(handle);

                future.whenComplete((vv, ex) -> SwingUtilities.invokeLater(() -> {
                    java.util.List<RunningJobHandle> list = runningExecutions.get(noteId);
                    if (list != null) {
                        list.remove(handle);
                        if (list.isEmpty()) {
                            runningExecutions.remove(noteId);
                            panel.setExecutionRunning(false);
                            statusLabel.setText("就绪");
                        }
                    }
                    updateExecutionButtons();
                }));

                return future;
            });
        }

        updateExecutionButtons();
    }

    private void renderResult(long noteId, EditorTabPanel panel, SqlExecResult result, QueryResultPanel existingPanel) {
        QueryResultPanel target = existingPanel != null ? existingPanel : new QueryResultPanel(result, result.getSql());
        target.render(result);
        metadataService.handleSqlSucceeded(result.getSql());
        if (existingPanel == null) {
            int idx = nextResultIndex(noteId);
            String tabTitle = windowMode ? "结果" + idx : panel.getNote().getTitle() + "-结果" + idx;
            if (windowMode) {
                panel.addLocalResultPanel(tabTitle, target, result.getSql());
            } else {
                SharedResultView view = ensureSharedView();
                view.addResultTab(tabTitle, target, result.getSql());
                expandSharedResults();
            }
        }
    }

    private int nextResultIndex(long noteId) {
        return resultTabCounters.computeIfAbsent(noteId, k -> new AtomicInteger(1)).getAndIncrement();
    }

    private QueryResultPanel addPendingResultPanel(long noteId, EditorTabPanel panel, String sql) {
        QueryResultPanel qp = QueryResultPanel.pending(sql);
        int idx = nextResultIndex(noteId);
        String tabTitle = windowMode ? "结果" + idx : panel.getNote().getTitle() + "-结果" + idx;
        if (windowMode) {
            panel.addLocalResultPanel(tabTitle, qp, sql);
        } else {
            SharedResultView view = ensureSharedView();
            view.addResultTab(tabTitle, qp, sql);
            expandSharedResults();
        }
        return qp;
    }

    private void handleStatusUpdate(RunningJobHandle handle, AsyncJobStatus status, EditorTabPanel panel) {
        if (handle == null || status == null) return;
        handle.jobId = status.getJobId();
        if (handle.panel != null) {
            handle.panel.updateProgress(status);
        }
        String progress = status.getProgressPercent() != null ? status.getProgressPercent() + "%" : "-";
        statusLabel.setText("任务 " + status.getStatus() + " " + progress);
        if ("SUCCEEDED".equalsIgnoreCase(status.getStatus())
                || "FAILED".equalsIgnoreCase(status.getStatus())
                || "CANCELLED".equalsIgnoreCase(status.getStatus())) {
            panel.setExecutionRunning(false);
        }
        updateExecutionButtons();
    }

    private void renderError(long noteId, EditorTabPanel panel, String sql, Exception ex, QueryResultPanel existingPanel) {
        DatabaseErrorInfo errorInfo = ex instanceof SqlExecutionException se ? se.getErrorInfo() : null;
        Integer statementIndex = ex instanceof SqlExecutionException se ? se.getStatementIndex() : null;
        String sqlFragment = ex instanceof SqlExecutionException se ? se.getSqlFragment() : null;
        String message = ErrorDisplayFormatter.chooseDisplayMessage(errorInfo, null, ex.getMessage(), "FAILED");

        if (existingPanel != null) {
            existingPanel.renderError(errorInfo, message, statementIndex, sqlFragment);
        } else {
            QueryResultPanel qp = QueryResultPanel.pending(sql);
            qp.renderError(errorInfo, message, statementIndex, sqlFragment);
            String tabTitle = windowMode ? "错误" + nextResultIndex(noteId)
                    : panel.getNote().getTitle() + "-错误" + nextResultIndex(noteId);
            if (windowMode) {
                panel.addLocalResultPanel(tabTitle, qp, sql);
            } else {
                SharedResultView view = ensureSharedView();
                view.addResultTab(tabTitle, qp, sql);
                expandSharedResults();
            }
        }
        OperationLog.log("SQL 执行失败: " + OperationLog.abbreviate(sql, 120) + " | " + message);
    }

    private SharedResultView ensureSharedView() {
        return sharedResultView;
    }

    private void expandSharedResults() {
        showBottomPanel();
        bottomTabbedPane.setSelectedComponent(sharedResultsWrapper);
    }

    private void collapseSharedResults() {
        if (bottomTabbedPane != null) {
            bottomTabbedPane.setSelectedComponent(sharedResultsWrapper);
        }
    }

    private static class RunningJobHandle {
        CompletableFuture<Void> future;
        String jobId;
        QueryResultPanel panel;
        String sql;
    }

    private static class SharedResultView {
        final JPanel wrapper = new JPanel(new BorderLayout());
        final JTabbedPane tabs = new JTabbedPane();
        final JScrollPane scroll;

        SharedResultView() {
            tabs.setBorder(BorderFactory.createEmptyBorder());
            scroll = new JScrollPane(tabs);
            scroll.setBorder(BorderFactory.createEmptyBorder());
            wrapper.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createMatteBorder(1, 0, 0, 0, new Color(228, 231, 236)),
                    new EmptyBorder(8, 0, 0, 0)));
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
        java.util.List<RunningJobHandle> list = runningExecutions.get(noteId);
        if (list != null) {
            for (RunningJobHandle handle : list) {
                if (handle.jobId != null) {
                    sqlExecutionService.cancelJob(handle.jobId, "用户取消");
                }
            }
        }
        panel.setExecutionRunning(false);
        statusLabel.setText("已发送取消请求");
        updateExecutionButtons();
        OperationLog.log("停止执行: " + panel.getNote().getTitle());
    }

    private void stopIfRunning() {
        if (hasRunningExecution()) {
            stopCurrentExecution();
        }
    }

    private boolean hasRunningExecution() {
        EditorTabPanel panel = getCurrentPanel();
        if (panel == null) {
            return false;
        }
        long noteId = panel.getNote().getId();
        java.util.List<RunningJobHandle> list = runningExecutions.get(noteId);
        if (list == null) {
            return false;
        }
        for (RunningJobHandle handle : list) {
            if (handle.future != null && !handle.future.isDone()) {
                return true;
            }
        }
        return false;
    }

    private int sanitizePageSize(int desired) {
        int fallback = Config.getDefaultPageSize();
        int max = Config.getMaxPageSize();
        if (desired < 1) {
            OperationLog.log("pageSize=" + desired + " 无效，使用默认值 " + fallback);
            return fallback;
        }
        if (desired > max) {
            OperationLog.log("pageSize=" + desired + " 超出上限，已裁剪到 " + max);
            return max;
        }
        return desired;
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
        button.setBorder(new javax.swing.border.LineBorder(new Color(46, 170, 220), 2, true));
        button.setMargin(new Insets(6, 10, 6, 10));
    }

    private void refreshToolbarButtonStyles() {
        Color active = new Color(46, 170, 220);
        if (executeButton != null) {
            executeButton.setBorder(new javax.swing.border.LineBorder(
                    executeButton.isEnabled() ? active : Color.DARK_GRAY, 2, true));
        }
        if (stopButton != null) {
            stopButton.setBorder(new javax.swing.border.LineBorder(
                    stopButton.isEnabled() ? active : Color.DARK_GRAY, 2, true));
        }
    }

    private void openManageDialog() {
        // 将图标提供与“更换图标”回调传入管理界面
        ManageNotesDialog dialog = new ManageNotesDialog(
                this,
                noteRepository,
                this::openNoteWithNavigation,
                this::getOrCreateNoteIcon,
                this::regenerateNoteIcon
        );
        dialog.setVisible(true);
    }

    private void openTrashBin() {
        getTrashBinDialog().open();
    }

    private void importNoteFromFile() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("导入本地笔记 (*.sql, *.md)");
        chooser.setFileFilter(
                new javax.swing.filechooser.FileNameExtensionFilter(
                        "SQL/Markdown", "sql", "md", "txt"));
        int result = chooser.showOpenDialog(this);
        if (result == JFileChooser.APPROVE_OPTION) {
            Path file = chooser.getSelectedFile().toPath();
            try {
                String content = Files.readString(file, StandardCharsets.UTF_8);
                String name = file.getFileName().toString();
                int dot = name.lastIndexOf('.');
                String title = dot > 0 ? name.substring(0, dot) : name;
                DatabaseType type = (DatabaseType) JOptionPane.showInputDialog(
                        this,
                        "选择数据库类型",
                        "导入笔记",
                        JOptionPane.QUESTION_MESSAGE,
                        null,
                        DatabaseType.values(),
                        DatabaseType.POSTGRESQL
                );
                if (type != null) {
                    Note note = noteRepository.create(title, type);
                    noteRepository.updateContent(note, content);
                    openNoteInCurrentMode(note);
                }
            } catch (Exception ex) {
                JOptionPane.showMessageDialog(this, "导入失败: " + ex.getMessage());
            }
        }
    }

    private void openEditorSettings() {
        SqlEditorSettingsDialog dialog = new SqlEditorSettingsDialog(
                this,
                convertFullWidth,
                styleRepository,
                styleRepository.listAll(),
                currentStyle
        );
        dialog.setVisible(true);
        if (dialog.isConfirmed()) {
            convertFullWidth = dialog.isConvertFullWidthEnabled();
            appStateRepository.saveFullWidthOption(convertFullWidth);
            panelCache.values().forEach(p -> p.setFullWidthConversionEnabled(convertFullWidth));
            availableStyles = styleRepository.listAll();
            dialog.getSelectedStyle().ifPresent(style -> {
                currentStyle = findStyleByName(style.getName()).orElse(style);
                appStateRepository.saveCurrentStyleName(currentStyle.getName());
                panelCache.values().forEach(p -> {
                    if (p.getNote().getStyleName() == null || p.getNote().getStyleName().isBlank()) {
                        p.applyStyle(currentStyle);
                    }
                    refreshPanelStyleMenu(p);
                });
            });
            panelCache.values().forEach(this::refreshPanelStyleMenu);
        }
    }

    private EditorStyle resolveStyleForNote(Note note) {
        if (note != null && note.getStyleName() != null && !note.getStyleName().isBlank()) {
            return findStyleByName(note.getStyleName()).orElse(currentStyle);
        }
        return currentStyle;
    }

    private Optional<EditorStyle> findStyleByName(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        return availableStyles.stream()
                .filter(s -> s.getName().equalsIgnoreCase(name))
                .findFirst();
    }

    private void refreshPanelStyleMenu(EditorTabPanel panel) {
        if (panel == null) return;
        panel.refreshStyleMenu(availableStyles, currentStyle,
                style -> applyStyleToPanel(panel, style),
                () -> resetPanelStyle(panel));
    }

    private void applyStyleToPanel(EditorTabPanel panel, EditorStyle style) {
        try {
            panel.applyStyle(style);
            noteRepository.updateStyleName(panel.getNote(), style.getName());
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "应用样式失败: " + ex.getMessage());
        }
    }

    private void resetPanelStyle(EditorTabPanel panel) {
        try {
            noteRepository.updateStyleName(panel.getNote(), "");
            panel.applyStyle(currentStyle);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "重置样式失败: " + ex.getMessage());
        }
    }

    private void ensureBuiltinStyles() {
        List<EditorStyle> presets = List.of(
                new EditorStyle("默认", 14, "#FFFFFF", "#000000", "#CCE8FF", "#000000", "#005CC5", "#032F62", "#6A737D", "#1C7C54", "#000000", "#6F42C1", "#005CC5", "#24292E", "#D73A49", "#F6F8FA", "#FFDD88"),
                new EditorStyle("经典亮色", 14, "#FDFDFD", "#1E1E1E", "#CCE8FF", "#111111", "#0066BF", "#B03060", "#8A8A8A", "#1C7C54", "#1E1E1E", "#005F87", "#0F4C81", "#1E1E1E", "#AF00DB", "#F2F8FF", "#FFA657"),
                new EditorStyle("柔和护眼", 14, "#F6FBF4", "#2E3B2E", "#D9F2E6", "#3B3B3B", "#2E7D32", "#2E7D6C", "#6D8B74", "#33691E", "#3E4C3E", "#1565C0", "#2E7D32", "#1B3C35", "#8E24AA", "#E8F5E9", "#81C784"),
                new EditorStyle("暗夜海蓝", 14, "#1E1E2A", "#DADADA", "#2D3B55", "#A0A0A0", "#82AAFF", "#C3E88D", "#697098", "#F78C6C", "#E0E0E0", "#C792EA", "#7FDBCA", "#D0D0D0", "#FFCB6B", "#2A2E3F", "#89DDFF"),
                new EditorStyle("Monokai 经典", 14, "#272822", "#F8F8F2", "#49483E", "#F8F8F0", "#F92672", "#E6DB74", "#75715E", "#AE81FF", "#F8F8F2", "#66D9EF", "#A6E22E", "#F8F8F2", "#FD971F", "#3E3D32", "#A6E22E"),
                new EditorStyle("Solarized Dark", 14, "#002B36", "#839496", "#073642", "#93A1A1", "#268BD2", "#2AA198", "#586E75", "#D33682", "#93A1A1", "#B58900", "#859900", "#93A1A1", "#CB4B16", "#073642", "#6C71C4"),
                new EditorStyle("终端绿黑", 14, "#0C0C0C", "#0EE676", "#1F1F1F", "#0EE676", "#1E90FF", "#7CFC00", "#3A9D23", "#66D9EF", "#E5E510", "#61AFEF", "#56B6C2", "#C5C8C6", "#98C379", "#1A1A1A", "#50FA7B")
        );
        for (EditorStyle style : presets) {
            try {
                styleRepository.upsert(style);
            } catch (Exception ex) {
                OperationLog.log("写入内置样式失败: " + ex.getMessage());
            }
        }
    }

    private void initTheme() {
        List<ThemeOption> options = themeManager.getOptions();
        if (options.isEmpty()) {
            return;
        }
        String defaultId = options.get(0).getId();
        String saved = appStateRepository.loadCurrentThemeName(defaultId);
        currentTheme = themeManager.findById(saved).orElse(options.get(0));
        themeManager.applyThemeAsync(currentTheme, this::applyUiDefaults);
    }

    private void switchTheme(ThemeOption option) {
        if (option == null) {
            return;
        }
        if (currentTheme != null && currentTheme.getId().equals(option.getId())) {
            return;
        }
        currentTheme = option;
        appStateRepository.saveCurrentThemeName(option.getId());
        themeManager.applyThemeAsync(option, () -> {
            applyUiDefaults();
            repaint();
        });
    }

    private void applyUiDefaults() {
        FontUIResource uiFont = new FontUIResource(uiBaseFont);
        java.util.Enumeration<Object> keys = UIManager.getDefaults().keys();
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            Object value = UIManager.get(key);
            if (value instanceof Font) {
                UIManager.put(key, uiFont);
            }
        }
        UIManager.put("defaultFont", uiFont);
        UIManager.put("Table.rowHeight", 24);
        UIManager.put("Tree.rowHeight", 22);
        UIManager.put("Table.selectionBackground", new Color(0xDCE6F5));
        UIManager.put("Table.selectionForeground", new Color(0x0F1F3A));
        UIManager.put("Tree.selectionBackground", new Color(0xDCE6F5));
        UIManager.put("Tree.selectionForeground", new Color(0x0F1F3A));
        UIManager.put("SplitPane.border", BorderFactory.createEmptyBorder());
        UIManager.put("ScrollPane.border", BorderFactory.createEmptyBorder());
        UIManager.put("TabbedPane.contentBorderInsets", new Insets(8, 8, 8, 8));
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

    private String getPanelDisplayTitle(EditorTabPanel panel) {
        if (panel == null) {
            return "";
        }
        Object custom = panel.getClientProperty("customTitle");
        if (custom instanceof String title && !title.isBlank()) {
            return appendTemporaryMark(title, panel);
        }
        return appendTemporaryMark(panel.getNote().getTitle(), panel);
    }

    private String getPanelTooltip(EditorTabPanel panel) {
        if (panel == null) {
            return null;
        }
        Object tooltip = panel.getClientProperty("customTooltip");
        if (tooltip instanceof String tip && !tip.isBlank()) {
            return tip;
        }
        return null;
    }

    private void refreshPanelTitle(EditorTabPanel panel) {
        if (panel == null) {
            return;
        }
        String displayTitle = getPanelDisplayTitle(panel);
        String tooltip = getPanelTooltip(panel);
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            EditorTabPanel framePanel = extractPanelFromFrame(frame);
            if (framePanel == panel) {
                frame.setTitle(displayTitle);
                frame.setToolTipText(tooltip);
                break;
            }
        }
        int idx = tabbedPane.indexOfComponent(panel);
        if (idx >= 0) {
            tabbedPane.setTitleAt(idx, displayTitle);
            tabbedPane.setToolTipTextAt(idx, tooltip);
        }
    }

    private void updateTabTitle(EditorTabPanel panel, String title) {
        if (panel != null && panel.getClientProperty("customTitle") != null) {
            return;
        }
        int idx = tabbedPane.indexOfComponent(panel);
        if (idx >= 0) {
            tabbedPane.setTitleAt(idx, appendTemporaryMark(title, panel));
        }
    }

    private void updateTitleForPanel(String title, Long noteId) {
        long targetId = noteId != null ? noteId : -1L;
        panelCache.values().forEach(p -> {
            if (p.getNote().getId() == targetId) {
                if (p.getClientProperty("customTitle") == null) {
                    updateTabTitle(p, title);
                }
                for (JInternalFrame frame : desktopPane.getAllFrames()) {
                    EditorTabPanel framePanel = extractPanelFromFrame(frame);
                    if (framePanel == p) {
                        if (p.getClientProperty("customTitle") == null) {
                            frame.setTitle(appendTemporaryMark(title, p));
                        }
                        break;
                    }
                }
            }
        });
        persistOpenFrames();
    }

    private String appendTemporaryMark(String title, EditorTabPanel panel) {
        if (panel != null && panel.isTemporary()) {
            return title + " (临时)";
        }
        return title;
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
            } catch (Exception ignored) {
            }
            frames[i].setBounds(c * w, r * h, w, h);
        }
    }

    private void updateAutosaveTime(String text) {
        SwingUtilities.invokeLater(() -> autosaveLabel.setText("自动保存: " + text));
    }

    private void updateTaskCount(int count) {
        SwingUtilities.invokeLater(() -> taskLabel.setText("后台任务: " + count));
    }

    private void installGlobalShortcuts() {
        InputMap inputMap = getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
        ActionMap actionMap = getRootPane().getActionMap();
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_W, InputEvent.ALT_DOWN_MASK), "toggle_quick_snippet_dialog");
        actionMap.put("toggle_quick_snippet_dialog", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                getQuickSqlSnippetDialog().toggleVisible();
            }
        });
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_Q, InputEvent.ALT_DOWN_MASK), "toggle_exec_history_dialog");
        actionMap.put("toggle_exec_history_dialog", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                getExecutionHistoryDialog().toggleVisible();
            }
        });
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_W, InputEvent.CTRL_DOWN_MASK), "close_current_container");
        actionMap.put("close_current_container", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                closeCurrentContainer();
            }
        });
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_L, InputEvent.CTRL_DOWN_MASK), "focus_editor_area");
        actionMap.put("focus_editor_area", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                focusEditorArea();
            }
        });
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "stop_execution_if_running");
        actionMap.put("stop_execution_if_running", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                stopIfRunning();
            }
        });
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_F6, 0), "cycle_focus");
        actionMap.put("cycle_focus", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                focusNextMajorArea();
            }
        });
        KeyboardFocusManager.getCurrentKeyboardFocusManager()
                .addKeyEventDispatcher(e -> {
                    if (e.getID() == KeyEvent.KEY_PRESSED) {
                        if (e.isAltDown() && e.getKeyCode() == KeyEvent.VK_E) {
                            toggleSuggestionMode();
                            return true;
                        }
                        if (e.isControlDown() && e.getKeyCode() == KeyEvent.VK_TAB) {
                            navigateEditors(!e.isShiftDown());
                            return true;
                        }
                        if (e.isControlDown() && e.getKeyCode() == KeyEvent.VK_ENTER) {
                            executeCurrentSql();
                            return true;
                        }
                    }
                    return false;
                });
    }

    private QuickSqlSnippetDialog getQuickSqlSnippetDialog() {
        if (quickSqlSnippetDialog == null) {
            quickSqlSnippetDialog = new QuickSqlSnippetDialog(this, sqlSnippetRepository);
        }
        return quickSqlSnippetDialog;
    }

    private ExecutionHistoryDialog getExecutionHistoryDialog() {
        if (executionHistoryDialog == null) {
            executionHistoryDialog = new ExecutionHistoryDialog(this, sqlHistoryRepository, sqlSnippetRepository, this::insertSqlToCurrentEditor);
        }
        return executionHistoryDialog;
    }

    private TrashBinDialog getTrashBinDialog() {
        if (trashBinDialog == null) {
            trashBinDialog = new TrashBinDialog(this, noteRepository, this::getOrCreateNoteIcon);
        }
        return trashBinDialog;
    }

    private void toggleSuggestionMode() {
        suggestionEnabled = !suggestionEnabled;
        suggestionStateLabel.setText("联想: " + (suggestionEnabled ? "开启" : "关闭"));
        statusLabel.setText(suggestionEnabled ? "联想已开启" : "联想已关闭");
        panelCache.values().forEach(p -> {
            p.setSuggestionEnabled(suggestionEnabled);
            if (!suggestionEnabled) {
                p.hideSuggestionPopup("suggestionDisabled");
            }
        });
    }

    private void insertSqlToCurrentEditor(String sql) {
        if (sql == null || sql.isBlank()) {
            return;
        }
        EditorTabPanel panel = getCurrentPanel();
        if (panel == null) {
            JOptionPane.showMessageDialog(this, "请先打开一个笔记窗口");
            return;
        }
        panel.insertSqlAtCaret(sql);
    }

    private void showObjectBrowser() {
        if (objectBrowserDialog == null) {
            objectBrowserDialog = new ObjectBrowserDialog(MainFrame.this, metadataService, pgRoutineService,
                    new ObjectBrowserDialog.RoutineActionHandler() {
                        @Override
                        public void openRoutine(Component source, RoutineInfo info, boolean editable) {
                            openRoutineEditor(info, editable, source);
                        }

                        @Override
                        public void runRoutine(Component source, RoutineInfo info) {
                            runRoutineWithDialog(info, source);
                        }
                    });
        }
        objectBrowserDialog.setVisible(true);
        objectBrowserDialog.reload();
    }

    private void showShortcutDialog() {
        String content = "快捷键列表\n" +
                "Ctrl+Enter  执行当前 SQL\n" +
                "Ctrl+W      关闭当前标签\n" +
                "Ctrl+Tab    切换到下一个标签/窗口\n" +
                "Ctrl+Shift+Tab 切换到上一个标签/窗口\n" +
                "Ctrl+L      聚焦到编辑器\n" +
                "Esc         停止/取消正在执行的任务\n" +
                "F6          对象树/编辑器/结果区循环焦点\n" +
                "Alt+W       SQL 片段库\n" +
                "Alt+Q       执行历史\n" +
                "Ctrl+,      SQL 编辑器选项";
        JDialog dialog = new JDialog(this, "Shortcuts", true);
        JTextArea area = new JTextArea(content);
        area.setEditable(false);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        area.setBorder(new EmptyBorder(12, 12, 12, 12));
        dialog.setLayout(new BorderLayout());
        dialog.add(new JScrollPane(area), BorderLayout.CENTER);
        dialog.setSize(420, 340);
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private void refreshObjectBrowserTree() {
        if (objectBrowserDialog != null && objectBrowserDialog.isVisible()) {
            SwingUtilities.invokeLater(() -> objectBrowserDialog.reload());
        }
        if (navigationBrowserPanel != null) {
            SwingUtilities.invokeLater(() -> navigationBrowserPanel.reload());
        }
    }

    private void unregisterRoutinePanel(EditorTabPanel panel) {
        if (panel == null) {
            return;
        }
        Object oidObj = panel.getClientProperty("routineOid");
        if (oidObj instanceof Long oid) {
            routineTabIndex.remove(oid);
        }
    }

    private void openRoutineEditor(RoutineInfo info, boolean editable, Component ownerSource) {
        if (info == null) {
            return;
        }
        EditorTabPanel existing = routineTabIndex.get(info.oid());
        if (existing != null) {
            if (!existing.isShowing()) {
                routineTabIndex.remove(info.oid());
            } else {
                SwingUtilities.invokeLater(() -> {
                    selectContainerForPanel(existing);
                    existing.focusEditorArea();
                    existing.setReadOnly(!editable);
                });
                return;
            }
        }
        showRoutineDdlAndOpen(info, editable, ownerSource, null);
    }

    private void runRoutineWithDialog(RoutineInfo info, Component ownerSource) {
        showRoutineDdlAndOpen(info, false, ownerSource, panel -> {
            Window owner = resolveOwnerWindow(ownerSource);
            RoutineRunDialog dialog = new RoutineRunDialog(owner instanceof Frame ? (Frame) owner : this, info);
            dialog.setVisible(true);
            RoutineRunDialog.Result result = dialog.getResult();
            if (result == null || !result.confirmed() || result.sql() == null || result.sql().isBlank()) {
                return;
            }
            executeRoutineSql(panel, result.sql(), info, null, msg -> locateErrorInEditor(panel, msg));
        });
    }

    private void showRoutineDdlAndOpen(RoutineInfo info, boolean editable, Component ownerSource,
                                       java.util.function.Consumer<EditorTabPanel> afterOpen) {
        if (info == null) {
            return;
        }
        Window owner = resolveOwnerWindow(ownerSource);
        JDialog progress = showProgressDialog(owner, "正在获取源码...");
        pgRoutineService.loadRoutineDdl(info.oid()).whenComplete((ddl, ex) -> SwingUtilities.invokeLater(() -> {
            if (progress != null) {
                progress.dispose();
            }
            if (ex != null) {
                JOptionPane.showMessageDialog(this, "获取源码失败: " + ex.getMessage());
                return;
            }
            Note tempNote = createTemporaryNote(info, ddl);
            TemporaryNoteWindow window = new TemporaryNoteWindow(
                    owner,
                    noteRepository,
                    metadataService,
                    tempNote,
                    resolveStyleForNote(tempNote),
                    convertFullWidth,
                    defaultPageSize,
                    this::openPermanentNote,
                    info.displayName());
            EditorTabPanel panel = window.getEditorPanel();
            panel.setTextContent(ddl);
            panel.configureRoutineContext("例程: " + info.displayName(), editable, () -> publishRoutine(panel, info));
            panel.setReadOnly(!editable);
            window.setVisible(true);
            if (afterOpen != null) {
                afterOpen.accept(panel);
            }
            statusLabel.setText("已载入: " + info.displayName());
        }));
    }

    private Note createTemporaryNote(RoutineInfo info, String ddl) {
        String title = "临时查看: " + info.displayName();
        return createTemporaryNote(DatabaseType.POSTGRESQL, title, ddl == null ? "" : ddl);
    }

    private Note createTemporaryNote(DatabaseType type, String title, String content) {
        long now = System.currentTimeMillis();
        long tempId = -Math.abs(java.util.concurrent.ThreadLocalRandom.current().nextLong(Long.MAX_VALUE));
        Note note = new Note(tempId, title, content == null ? "" : content, type, now, now,
                "", false, false, 0, "");
        note.setTemporary(true);
        return note;
    }

    void openPermanentNote(Note note) {
        openNoteInCurrentMode(note);
    }

    private String buildRoutineNoteTitle(RoutineInfo info) {
        String base = info.objectName() == null ? ("routine_" + info.oid()) : info.objectName();
        if (!noteRepository.titleExists(base, null)) {
            return base;
        }
        return base + " #" + info.oid();
    }

    private void markRoutinePanel(EditorTabPanel panel, RoutineInfo info, boolean editable) {
        if (panel == null || info == null) {
            return;
        }
        panel.putClientProperty("routineOid", info.oid());
        panel.putClientProperty("customTitle", info.objectName());
        String tooltip = info.displayName();
        if (tooltip != null && !tooltip.isBlank()) {
            panel.putClientProperty("customTooltip", tooltip);
        }
        routineTabIndex.put(info.oid(), panel);
        refreshPanelTitle(panel);
        panel.setReadOnly(!editable);
    }

    private JDialog showProgressDialog(Window owner, String message) {
        JDialog dialog = new JDialog(owner, "执行中", false);
        dialog.setLayout(new BorderLayout());
        dialog.add(new JLabel(message), BorderLayout.NORTH);
        JProgressBar bar = new JProgressBar();
        bar.setIndeterminate(true);
        dialog.add(bar, BorderLayout.CENTER);
        dialog.setSize(320, 100);
        dialog.setLocationRelativeTo(owner);
        dialog.setAlwaysOnTop(true);
        dialog.setVisible(true);
        return dialog;
    }

    private Window resolveOwnerWindow(Component source) {
        return WindowOwnerResolver.resolve(source, this);
    }

    private void publishRoutine(EditorTabPanel panel, RoutineInfo info) {
        if (panel == null || info == null) {
            return;
        }
        String ddl = panel.getSqlText();
        if (ddl == null || ddl.isBlank()) {
            JOptionPane.showMessageDialog(this, "没有可发布的源码");
            return;
        }
        executeRoutineSql(panel, ddl, info, this::refreshObjectBrowserTree, msg -> locateErrorInEditor(panel, msg));
    }

    private void executeRoutineSql(EditorTabPanel panel, String sql, RoutineInfo info,
                                   Runnable onSuccess, java.util.function.Consumer<String> onError) {
        if (panel == null || sql == null || sql.isBlank()) {
            JOptionPane.showMessageDialog(this, "SQL 为空，无法执行");
            return;
        }
        long noteId = panel.getNote().getId();
        if (runningExecutions.getOrDefault(noteId, List.of()).size() > 0) {
            JOptionPane.showMessageDialog(this, "当前窗口正在执行，请先停止或等待结束");
            return;
        }

        if (windowMode) {
            panel.clearLocalResults();
        } else {
            SharedResultView target = ensureSharedView();
            target.clear();
            collapseSharedResults();
        }
        resultTabCounters.put(noteId, new java.util.concurrent.atomic.AtomicInteger(1));
        statusLabel.setText("执行中...");
        panel.setExecutionRunning(true);
        runningExecutions.putIfAbsent(noteId, new CopyOnWriteArrayList<>());
        List<RunningJobHandle> execList = runningExecutions.get(noteId);

        QueryResultPanel pendingPanel = addPendingResultPanel(noteId, panel, sql);
        RunningJobHandle handle = new RunningJobHandle();
        handle.panel = pendingPanel;
        handle.sql = sql;

        CompletableFuture<Void> future = sqlExecutionService.execute(
                sql,
                panel.getSelectedDbUser(),
                panel.getPreferredPageSize(),
                res -> {
                    SqlExecResult normalized = columnOrderDecider.reorder(res,
                            refreshed -> SwingUtilities.invokeLater(() -> renderResult(noteId, panel, refreshed, pendingPanel)));
                    SwingUtilities.invokeLater(() -> {
                        renderResult(noteId, panel, normalized, pendingPanel);
                        if (onSuccess != null) {
                            onSuccess.run();
                        }
                    });
                },
                ex -> SwingUtilities.invokeLater(() -> {
                    renderError(noteId, panel, sql, ex, pendingPanel);
                    if (onError != null) {
                        onError.accept(ex.getMessage());
                    }
                }),
                status -> SwingUtilities.invokeLater(() -> handleStatusUpdate(handle, status, panel))
        );

        handle.future = future;
        execList.add(handle);

        future.whenComplete((vv, ex) -> SwingUtilities.invokeLater(() -> {
            List<RunningJobHandle> list = runningExecutions.get(noteId);
            if (list != null) {
                list.remove(handle);
                if (list.isEmpty()) {
                    runningExecutions.remove(noteId);
                    panel.setExecutionRunning(false);
                    statusLabel.setText("就绪");
                }
            }
            updateExecutionButtons();
        }));

        updateExecutionButtons();
    }

    private void locateErrorInEditor(EditorTabPanel panel, String message) {
        if (panel == null || message == null || message.isBlank()) {
            return;
        }
        Matcher lineMatcher = LINE_PATTERN.matcher(message);
        if (lineMatcher.find()) {
            try {
                int line = Integer.parseInt(lineMatcher.group(1));
                panel.highlightLine(line);
                return;
            } catch (NumberFormatException ignored) {
            }
        }
        Matcher posMatcher = POSITION_PATTERN.matcher(message);
        if (posMatcher.find()) {
            try {
                int pos = Integer.parseInt(posMatcher.group(1));
                panel.highlightPosition(pos);
            } catch (NumberFormatException ignored) {
            }
        }
    }

    private void handleMetadataRefreshResult(MetadataService.MetadataRefreshResult result) {
        if (result == null) return;
        if (result.success()) {
            String text = "元数据: " + result.totalObjects();
            if (result.totalColumns() > 0) {
                text += " / 列 " + result.totalColumns();
            }
            if (result.changedObjects() > 0) {
                text += " (变动 " + result.changedObjects() + ")";
            }
            metadataLabel.setText(text);
            log.info("UI 展示元数据计数: {} columns={}", result.totalObjects(), result.totalColumns());
            OperationLog.log("UI 展示元数据计数: " + result.totalObjects()
                    + " 列: " + result.totalColumns()
                    + " 批次: " + result.batches()
                    + " 用时: " + result.durationMillis() + "ms");
            if (!hasRunningExecutions()) {
                statusLabel.setText("元数据已更新");
            }
            refreshObjectBrowserTree();
        } else {
            String reason = result.message() == null ? "获取失败" : result.message();
            metadataLabel.setText("元数据: 使用本地缓存 (" + reason + ")");
            statusLabel.setText("元数据加载失败，使用本地库元数据");
        }
    }

    private boolean hasRunningExecutions() {
        return runningExecutions.values().stream()
                .anyMatch(list -> list != null && !list.isEmpty());
    }

    private void openNoteByTitle(String title) {
        try {
            Note target = noteRepository.findOrCreateByTitle(title);
            SwingUtilities.invokeLater(() -> openNoteInCurrentMode(target));
        } catch (Exception ex) {
            OperationLog.log("打开链接失败: " + title + " | " + ex.getMessage());
        }
    }

    private void enableDebugMode() {
        if (debugMode) {
            revealLogPanel();
            JOptionPane.showMessageDialog(this,
                    "Debug 模式已开启，已展示右侧日志栏。",
                    "Debug 模式",
                    JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        String input = JOptionPane.showInputDialog(this, "输入口令以开启 Debug 模式");
        if (input == null) {
            return;
        }
        if (DEBUG_SECRET.equals(input.trim())) {
            debugMode = true;
            revealLogPanel();
            OperationLog.log("Debug 模式已开启");
            JOptionPane.showMessageDialog(this,
                    "Debug 模式已开启，已展示右侧日志栏。",
                    "Debug 模式",
                    JOptionPane.INFORMATION_MESSAGE);
        } else {
            JOptionPane.showMessageDialog(this,
                    "口令不正确，无法开启 Debug 模式。",
                    "验证失败",
                    JOptionPane.WARNING_MESSAGE);
        }
    }

    private void revealLogPanel() {
        focusLogPanel();
    }

    private Path resolveDbPath() {
        Path dbPath = sqliteManager.getDbPath();
        if (dbPath.isAbsolute()) {
            return dbPath;
        }
        return Paths.get("").toAbsolutePath().resolve(dbPath);
    }

    // 记录并持久化最新备份目录
    private void updateLastBackupDir(Path dir) {
        if (dir == null) return;
        try {
            Path abs = dir.toAbsolutePath().normalize();
            lastBackupDir = abs;
            appStateRepository.saveStringOption(BACKUP_DIR_STATE_KEY, abs.toString());
        } catch (Exception ex) {
            OperationLog.log("保存备份目录失败: " + ex.getMessage());
        }
    }

    private Path createBackupCopy(Path source, Path targetDir, String tag) throws IOException {
        Files.createDirectories(targetDir);
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String baseName = source.getFileName().toString();
        String nameWithoutExt = baseName.endsWith(".db")
                ? baseName.substring(0, baseName.length() - 3)
                : baseName;
        Path target = targetDir.resolve(nameWithoutExt + "_" + tag + "_" + timestamp + ".db");
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        return target;
    }

    private void cancelScheduledBackup() {
        if (scheduledBackupTask != null) {
            scheduledBackupTask.cancel(false);
            scheduledBackupTask = null;
        }
        scheduledBackupDir = null;
        scheduledBackupLabel = null;
    }

    private void configureScheduledBackup() {
        String[] options = {"关闭定时备份", "每小时备份", "每天备份", "每周备份", "每月备份"};
        String choice = (String) JOptionPane.showInputDialog(
                this,
                "请选择定时备份频率：",
                "定时备份",
                JOptionPane.QUESTION_MESSAGE,
                null,
                options,
                options[0]);
        if (choice == null) {
            return;
        }
        if ("关闭定时备份".equals(choice)) {
            cancelScheduledBackup();
            OperationLog.log("已关闭定时备份");
            JOptionPane.showMessageDialog(this,
                    "定时备份已关闭",
                    "提示",
                    JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("选择定时备份目录");
        chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        chooser.setAcceptAllFileFilterUsed(false);
        // 记住最后一次备份目录
        if (lastBackupDir != null && Files.isDirectory(lastBackupDir)) {
            chooser.setCurrentDirectory(lastBackupDir.toFile());
            chooser.setSelectedFile(lastBackupDir.toFile());
        }
        int option = chooser.showSaveDialog(this);
        if (option != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path dir = chooser.getSelectedFile().toPath();
        updateLastBackupDir(dir);

        long periodHours;
        String label;
        switch (choice) {
            case "每小时备份" -> {
                periodHours = 1;
                label = "hourly";
            }
            case "每天备份" -> {
                periodHours = 24;
                label = "daily";
            }
            case "每周备份" -> {
                periodHours = 24 * 7;
                label = "weekly";
            }
            case "每月备份" -> {
                periodHours = 24 * 30;
                label = "monthly";
            }
            default -> {
                JOptionPane.showMessageDialog(this,
                        "不支持的备份频率",
                        "错误",
                        JOptionPane.ERROR_MESSAGE);
                return;
            }
        }
        schedulePeriodicBackup(dir, periodHours, label, choice);
    }

    private void schedulePeriodicBackup(Path dir, long periodHours, String label, String displayName) {
        cancelScheduledBackup();
        scheduledBackupDir = dir;
        scheduledBackupLabel = label;
        scheduledBackupTask = scheduledBackupExecutor.scheduleAtFixedRate(() -> {
            try {
                Path dbPath = resolveDbPath();
                if (!Files.exists(dbPath)) {
                    OperationLog.log("定时备份失败：未找到数据库文件 " + dbPath.toAbsolutePath());
                    return;
                }
                Path backup = createBackupCopy(
                        dbPath, scheduledBackupDir, "scheduled_" + scheduledBackupLabel);
                OperationLog.log("定时备份完成（" + scheduledBackupLabel + "): "
                        + backup.toAbsolutePath());
            } catch (Exception ex) {
                OperationLog.log("定时备份失败（" + scheduledBackupLabel + "): " + ex.getMessage());
            }
        }, periodHours, periodHours, TimeUnit.HOURS);
        JOptionPane.showMessageDialog(this,
                "已开启" + displayName + "，目录：" + dir.toAbsolutePath(),
                "定时备份已开启",
                JOptionPane.INFORMATION_MESSAGE);
    }

    private void backupLocalDatabase() {
        Path dbPath = resolveDbPath();
        if (!Files.exists(dbPath)) {
            JOptionPane.showMessageDialog(this,
                    "未找到本地数据库文件: " + dbPath,
                    "备份失败",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("选择备份目录");
        chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        chooser.setAcceptAllFileFilterUsed(false);
        // 使用上一次备份目录作为默认目录
        if (lastBackupDir != null && Files.isDirectory(lastBackupDir)) {
            chooser.setCurrentDirectory(lastBackupDir.toFile());
            chooser.setSelectedFile(lastBackupDir.toFile());
        }
        int option = chooser.showSaveDialog(this);
        if (option != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path dir = chooser.getSelectedFile().toPath();
        updateLastBackupDir(dir);
        try {
            Path backup = createBackupCopy(dbPath, dir, "backup");
            OperationLog.log("手动备份本地数据库到: " + backup.toAbsolutePath());
            JOptionPane.showMessageDialog(this,
                    "备份完成: " + backup.toAbsolutePath(),
                    "备份成功",
                    JOptionPane.INFORMATION_MESSAGE);
        } catch (IOException ex) {
            JOptionPane.showMessageDialog(this,
                    "备份失败: " + ex.getMessage(),
                    "备份失败",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    private void restoreLocalDatabase() {
        Path dbPath = resolveDbPath();
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("选择要恢复的数据库文件");
        chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        // 使用上一次备份目录作为默认目录
        if (lastBackupDir != null && Files.isDirectory(lastBackupDir)) {
            chooser.setCurrentDirectory(lastBackupDir.toFile());
        }
        int option = chooser.showOpenDialog(this);
        if (option != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path backupFile = chooser.getSelectedFile().toPath();
        if (!Files.exists(backupFile)) {
            JOptionPane.showMessageDialog(this,
                    "选定的文件不存在: " + backupFile,
                    "恢复失败",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }
        // 更新记忆目录为备份文件所在目录
        if (backupFile.getParent() != null) {
            updateLastBackupDir(backupFile.getParent());
        }

        String confirmation = JOptionPane.showInputDialog(
                this,
                "恢复前请输入：" + RESTORE_SECRET,
                "恢复确认",
                JOptionPane.WARNING_MESSAGE);
        if (confirmation == null) {
            return;
        }
        if (!RESTORE_SECRET.equals(confirmation.trim())) {
            JOptionPane.showMessageDialog(this,
                    "口令错误，已取消恢复。",
                    "恢复取消",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }
        try {
            Path backupDir = dbPath.getParent() != null
                    ? dbPath.getParent()
                    : Paths.get("").toAbsolutePath();
            Path autoBackup = null;
            if (Files.exists(dbPath)) {
                autoBackup = createBackupCopy(dbPath, backupDir, "auto_backup_before_restore");
            }
            Files.copy(backupFile, dbPath, StandardCopyOption.REPLACE_EXISTING);
            OperationLog.log("从备份恢复本地数据库: " + backupFile.toAbsolutePath());
            String message = "已从备份恢复数据库。";
            if (autoBackup != null) {
                message += " 当前数据库已自动备份到：" + autoBackup.toAbsolutePath();
            }
            JOptionPane.showMessageDialog(this,
                    message,
                    "恢复完成",
                    JOptionPane.INFORMATION_MESSAGE);
        } catch (IOException ex) {
            JOptionPane.showMessageDialog(this,
                    "恢复失败: " + ex.getMessage(),
                    "恢复失败",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    private void showUsageGuide() {
        String guide = """
# SQL Notebook 使用说明

## 联想模式
- Alt+E 可随时切换联想开关，状态栏实时显示“联想: 开启/关闭”，关闭后不再弹出列表且不会打断输入。
- 在输入表/视图、列、函数或存储过程位置时自动弹窗，输入连续字符、上下键与回车选择都会保持在当前位置过滤，只有回车确认或语句以分号结束时才收起。

## 双向链接
- 在 SQL 或笔记中插入 `[[标签]]` 以建立关联，执行 SQL 时系统会自动忽略方括号仅保留标签内容，避免影响 SQL 语义。
- 标签可以出现在任意位置，不限制输入顺序与行号。

## SQL 块执行
- Ctrl+Enter 会按照选择或光标所在语句块（或连续非空行块）执行，多个语句以分号拆分后串行提交。
- 执行/停止工具栏始终对应当前焦点窗口，可快速切换子窗口并控制 HTTPS 请求的开始或中止。

## 元数据与状态提示
- 启动后自动刷新对象元数据，状态栏会显示当前元数据量与本次变动数量；若刷新失败，将提示正在使用本地缓存。
- 右侧“操作日志”默认收拢，可展开查看元数据抓取、SQL 执行等详细轨迹。
""";
        JTextArea area = new JTextArea(guide);
        area.setEditable(false);
        area.setLineWrap(true);
        area.setWrapStyleWord(true);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        JScrollPane scroll = new JScrollPane(area);
        scroll.setPreferredSize(new Dimension(680, 420));
        JOptionPane.showMessageDialog(this, scroll, "使用说明",
                JOptionPane.INFORMATION_MESSAGE);
    }

    private void switchToPanelMode() {
        if (!windowMode) return;
        windowMode = false;
        windowModeItem.setSelected(false);
        panelModeItem.setSelected(true);
        OperationLog.log("切换到面板模式");
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            if (frame.getContentPane().getComponentCount() > 0
                    && frame.getContentPane().getComponent(0) instanceof EditorTabPanel panel) {
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
        OperationLog.log("切换到独立窗口模式");
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
            JInternalFrame frame = new JInternalFrame(
                    panel.getNote().getTitle(), true, true, true, true);
            // 独立窗口模式下，同样为子窗体设置持久化的随机绘制图标
            frame.setFrameIcon(getOrCreateNoteIcon(panel.getNote()));
            frame.setSize(600, 400);
            frame.setLocation(20 * offset, 20 * offset);
            offset++;
            frame.setVisible(true);
            frame.add(panel, BorderLayout.CENTER);
            installRenameHandler(frame, panel);
            desktopPane.add(frame);
            try {
                frame.setSelected(true);
            } catch (java.beans.PropertyVetoException ignored) {
            }
        }
        centerLayout.show(centerPanel, "window");
        sharedResultsWrapper.setVisible(false);
        persistOpenFrames();
        updateExecutionButtons();
    }
}
