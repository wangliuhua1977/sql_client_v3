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
import tools.sqlclient.util.LinkResolver;
import tools.sqlclient.util.OperationLog;

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
import java.awt.event.KeyEvent;

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
    private final JLabel suggestionStateLabel = new JLabel("联想: 开启");
    private final JLabel metadataLabel = new JLabel("元数据: -");
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
    // 笔记图标持久化：noteId -> 图标规格
    private final java.util.Map<Long, NoteIconSpec> noteIconSpecs = new java.util.HashMap<>();
    private final java.util.Random iconRandom = new java.util.Random();
    private boolean windowMode = true;
    private JRadioButtonMenuItem windowModeItem;
    private JRadioButtonMenuItem panelModeItem;
    private boolean convertFullWidth = true;
    private EditorStyle currentStyle;
    private boolean suggestionEnabled = true;
    private EditorTabPanel activePanel;
    private JButton executeButton;
    private JButton stopButton;
    private JSplitPane mainSplitPane;
    private JSplitPane horizontalSplit;
    private int sharedDividerLocation = -1;
    private JScrollPane sharedResultsScroll;
    private OperationLogPanel logPanel;

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
        // 加载笔记图标配置（noteId -> 图标规格）
        loadNoteIcons();
        // 加载主窗体图标：resources 目录下的 PNG，不存在则回退默认图标
        loadMainWindowIcon();
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
        OperationLog.setAppender(line -> logPanel.appendLine(line));
        metadataLabel.setText("元数据: " + metadataService.countCachedObjects());
        metadataService.refreshMetadataAsync(this::handleMetadataRefreshResult);
        installGlobalShortcuts();
        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent e) {
                persistOpenFrames();
            }
        });
        updateExecutionButtons();
    }

    // ==================== 主窗体图标 ====================

    /**
     * 从 resources 目录加载主窗体图标：
     * 约定路径：/icons/sql_client.png
     * 如果资源不存在，则回退为系统默认图标。
     */
    private void loadMainWindowIcon() {
        try {
            // Maven 工程中，resources 下的图标文件会被打成类路径资源
            java.net.URL url = getClass().getResource("/icons/sql_client.png");
            if (url != null) {
                Image img = new ImageIcon(url).getImage();
                setIconImage(img);
            } else {
                // 回退：尝试使用系统默认图标
                Icon uiIcon = UIManager.getIcon("FileView.computerIcon");
                if (uiIcon instanceof ImageIcon icon) {
                    setIconImage(icon.getImage());
                }
            }
        } catch (Exception ignore) {
            // 图标加载失败不影响主程序运行
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
                out.add(e.getKey() + "|" + e.getValue().toConfigString());
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
            persistNoteIcons();
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
        persistNoteIcons();
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
                        double py = cy + radius * Math.sin(angle);
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

        help.add(new JMenuItem(new AbstractAction("使用说明") {
            @Override
            public void actionPerformed(ActionEvent e) {
                showUsageGuide();
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
        desktopPane.addPropertyChangeListener("selectedFrame", evt -> {
            syncActivePanelWithSelection();
            updateExecutionButtons();
        });
        tabbedPane.addChangeListener(e -> {
            syncActivePanelWithSelection();
            updateExecutionButtons();
        });
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
        logPanel = new OperationLogPanel();
        horizontalSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, mainSplitPane, logPanel);
        horizontalSplit.setResizeWeight(0.78);
        horizontalSplit.setDividerSize(8);
        horizontalSplit.setContinuousLayout(true);
        horizontalSplit.setOneTouchExpandable(true);
        add(horizontalSplit, BorderLayout.CENTER);
        SwingUtilities.invokeLater(this::collapseSharedResults);
        SwingUtilities.invokeLater(() -> horizontalSplit.setDividerLocation(1.0));
        centerLayout.show(centerPanel, "window");
        restoreSession();
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
        status.setBorder(new EmptyBorder(4,8,4,8));
        JPanel left = new JPanel(new FlowLayout(FlowLayout.LEFT, 10,0));
        left.add(statusLabel);
        left.add(autosaveLabel);
        left.add(taskLabel);
        left.add(suggestionStateLabel);
        left.add(metadataLabel);
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
            OperationLog.log("新建笔记: " + title + " [" + type + "]");
            openNoteInCurrentMode(note);
            break;
        }
    }

    private EditorTabPanel getOrCreatePanel(Note note) {
        return panelCache.computeIfAbsent(note.getId(), id -> {
            EditorTabPanel p = new EditorTabPanel(noteRepository, metadataService,
                    this::updateAutosaveTime, this::updateTaskCount,
                    newTitle -> updateTitleForPanel(newTitle, id), note, convertFullWidth, currentStyle, this::onPanelFocused,
                    this::openNoteByTitle);
            p.setExecuteHandler(() -> executeCurrentSql(true));
            p.setSuggestionEnabled(suggestionEnabled);
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
        // 为子窗体设置持久化的随机绘制图标
        frame.setFrameIcon(getOrCreateNoteIcon(panel.getNote()));
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
        onPanelFocused(panel);
        updateExecutionButtons();
    }

    private void addTab(EditorTabPanel panel) {
        detachFromParent(panel);
        tabbedPane.addTab(panel.getNote().getTitle(), panel);
        int idx = tabbedPane.indexOfComponent(panel);
        // 面板模式下标签使用与子窗体相同的图标
        tabbedPane.setIconAt(idx, getOrCreateNoteIcon(panel.getNote()));
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
        if (selected != null) {
            activePanel = selected;
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
                    try { frame.setSelected(true); } catch (java.beans.PropertyVetoException ignored) {}
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

        java.util.List<String> statements = panel.getExecutableStatements(blockMode).stream()
                .map(LinkResolver::stripLinkTags)
                .filter(s -> s != null && !s.isBlank())
                .toList();
        if (statements.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请选择要执行的 SQL 语句");
            return;
        }

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
        // 按笔记 ID 记录运行中的 Future
        runningExecutions.putIfAbsent(noteId, new CopyOnWriteArrayList<>());
        List<CompletableFuture<?>> execList = runningExecutions.get(noteId);


        // 关键：构造一个“串行链”
        java.util.concurrent.CompletableFuture<Void> chain =
                java.util.concurrent.CompletableFuture.completedFuture(null);

        for (String stmt : statements) {
            final String sqlStmt = stmt;
            chain = chain.thenCompose(v -> {
                // 每次只在上一条执行完之后才真正发请求
                java.util.concurrent.CompletableFuture<Void> future = sqlExecutionService.execute(
                        sqlStmt,
                        res -> javax.swing.SwingUtilities.invokeLater(() -> renderResult(noteId, panel, res)),
                        ex  -> javax.swing.SwingUtilities.invokeLater(() -> renderError(noteId, panel, sqlStmt, ex.getMessage()))
                );

                execList.add(future);

                future.whenComplete((vv, ex) -> javax.swing.SwingUtilities.invokeLater(() -> {
                    java.util.List<java.util.concurrent.CompletableFuture<?>> list = runningExecutions.get(noteId);
                    if (list != null) {
                        list.remove(future);
                        if (list.isEmpty()) {
                            // 所有语句都执行完（或失败完）之后，才真正收尾
                            runningExecutions.remove(noteId);
                            panel.setExecutionRunning(false);
                            statusLabel.setText("就绪");
                        }
                    }
                    updateExecutionButtons();
                }));

                // 返回这一条的 Future，使后续 thenCompose 串在它后面
                return future;
            });
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
        OperationLog.log("SQL 执行失败: " + OperationLog.abbreviate(sql, 120) + " | " + message);
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
        OperationLog.log("停止执行: " + panel.getNote().getTitle());
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
        // 将图标提供与“更换图标”回调传入管理界面
        ManageNotesDialog dialog = new ManageNotesDialog(
                this,
                noteRepository,
                this::openNoteInCurrentMode,
                this::getOrCreateNoteIcon,
                this::regenerateNoteIcon
        );
        dialog.setVisible(true);
    }

    private void importNoteFromFile() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("导入本地笔记 (*.sql, *.md)");
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("SQL/Markdown", "sql", "md", "txt"));
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

    private void installGlobalShortcuts() {
        KeyboardFocusManager.getCurrentKeyboardFocusManager().addKeyEventDispatcher(e -> {
            if (e.getID() == KeyEvent.KEY_PRESSED && e.isAltDown() && e.getKeyCode() == KeyEvent.VK_E) {
                toggleSuggestionMode();
                return true;
            }
            return false;
        });
    }

    private void toggleSuggestionMode() {
        suggestionEnabled = !suggestionEnabled;
        suggestionStateLabel.setText("联想: " + (suggestionEnabled ? "开启" : "关闭"));
        statusLabel.setText(suggestionEnabled ? "联想已开启" : "联想已关闭");
        panelCache.values().forEach(p -> {
            p.setSuggestionEnabled(suggestionEnabled);
            if (!suggestionEnabled) {
                p.hideSuggestionPopup();
            }
        });
    }

    private void handleMetadataRefreshResult(MetadataService.MetadataRefreshResult result) {
        if (result == null) return;
        if (result.success()) {
            String text = "元数据: " + result.totalObjects();
            if (result.changedObjects() > 0) {
                text += " (变动 " + result.changedObjects() + ")";
            }
            metadataLabel.setText(text);
            if (!hasRunningExecutions()) {
                statusLabel.setText("元数据已更新");
            }
        } else {
            String reason = result.message() == null ? "获取失败" : result.message();
            metadataLabel.setText("元数据: 使用本地缓存 (" + reason + ")");
            statusLabel.setText("元数据加载失败，使用本地库元数据");
        }
    }

    private boolean hasRunningExecutions() {
        return runningExecutions.values().stream().anyMatch(list -> list != null && !list.isEmpty());
    }

    private void openNoteByTitle(String title) {
        try {
            Note target = noteRepository.findOrCreateByTitle(title);
            SwingUtilities.invokeLater(() -> openNoteInCurrentMode(target));
        } catch (Exception ex) {
            OperationLog.log("打开链接失败: " + title + " | " + ex.getMessage());
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
        JOptionPane.showMessageDialog(this, scroll, "使用说明", JOptionPane.INFORMATION_MESSAGE);
    }

    private void switchToPanelMode() {
        if (!windowMode) return;
        windowMode = false;
        windowModeItem.setSelected(false);
        panelModeItem.setSelected(true);
        OperationLog.log("切换到面板模式");
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
            JInternalFrame frame = new JInternalFrame(panel.getNote().getTitle(), true, true, true, true);
            // 独立窗口模式下，同样为子窗体设置持久化的随机绘制图标
            frame.setFrameIcon(getOrCreateNoteIcon(panel.getNote()));
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
