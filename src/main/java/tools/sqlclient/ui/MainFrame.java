package tools.sqlclient.ui;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.Note;
import tools.sqlclient.ui.ObjectBrowserDialog;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.plaf.basic.BasicInternalFrameUI;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主窗口，符合 Windows 11 扁平化风格，全部中文。
 */
public class MainFrame extends JFrame {
    private final JDesktopPane desktopPane = new JDesktopPane();
    private final JLabel statusLabel = new JLabel("就绪");
    private final JLabel autosaveLabel = new JLabel("自动保存: -");
    private final JLabel taskLabel = new JLabel("后台任务: 0");
    private final SQLiteManager sqliteManager;
    private final NoteRepository noteRepository;
    private final MetadataService metadataService;
    private final AtomicInteger untitledIndex = new AtomicInteger(1);

    public MainFrame() {
        super("SQL Notebook - 多标签 PG/Hive");
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());

        this.sqliteManager = new SQLiteManager(java.nio.file.Path.of("metadata.db"));
        this.metadataService = new MetadataService(java.nio.file.Path.of("metadata.db"));
        this.noteRepository = new NoteRepository(sqliteManager);
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
        file.add(new JMenuItem(new AbstractAction("打开笔记") {
            @Override
            public void actionPerformed(ActionEvent e) {
                openNoteFromDb();
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
                openNoteFromDb();
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
        add(desktopPane, BorderLayout.CENTER);
        createNote(DatabaseType.POSTGRESQL);
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
        String title = "未命名" + untitledIndex.getAndIncrement();
        Note note = noteRepository.create(title, type);
        addFrame(note);
    }

    private void addFrame(Note note) {
        EditorTabPanel[] holder = new EditorTabPanel[1];
        EditorTabPanel panel = new EditorTabPanel(noteRepository, metadataService,
                this::updateAutosaveTime, this::updateTaskCount,
                newTitle -> renameFrame(holder[0], newTitle), note);
        holder[0] = panel;
        JInternalFrame frame = new JInternalFrame(note.getTitle(), true, true, true, true);
        frame.setSize(600, 400);
        frame.setLocation(20 * desktopPane.getAllFrames().length, 20 * desktopPane.getAllFrames().length);
        frame.setVisible(true);
        frame.add(panel, BorderLayout.CENTER);
        installRenameHandler(frame, panel);
        installMaximizeTiling(frame);
        desktopPane.add(frame);
        try {
            frame.setSelected(true);
        } catch (java.beans.PropertyVetoException ignored) { }
    }

    private void installRenameHandler(JInternalFrame frame, EditorTabPanel panel) {
        BasicInternalFrameUI ui = (BasicInternalFrameUI) frame.getUI();
        if (ui != null && ui.getNorthPane() != null) {
            ui.getNorthPane().addMouseListener(new MouseAdapter() {
                @Override
                public void mouseClicked(MouseEvent e) {
                    if (e.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(e)) {
                        startInlineRename(frame, panel, e.getPoint());
                        e.consume();
                    }
                }
            });
        }
    }

    private void startInlineRename(JInternalFrame frame, EditorTabPanel panel, Point clickPoint) {
        BasicInternalFrameUI ui = (BasicInternalFrameUI) frame.getUI();
        JComponent north = ui != null ? (JComponent) ui.getNorthPane() : null;
        if (north == null) return;
        JTextField field = new JTextField(frame.getTitle());
        field.setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY));
        Dimension size = new Dimension(Math.max(140, field.getPreferredSize().width + 20), north.getHeight() - 6);
        field.setSize(size);
        int x = Math.max(6, clickPoint.x - size.width / 2);
        int y = 3;
        field.setLocation(x, y);
        north.add(field, 0);
        north.setLayout(null);
        north.revalidate();
        north.repaint();
        field.selectAll();
        field.requestFocusInWindow();

        Runnable commit = () -> {
            String text = field.getText();
            if (text != null && !text.isBlank()) {
                panel.rename(text.trim());
                frame.setTitle(text.trim());
            }
            north.remove(field);
            north.revalidate();
            north.repaint();
        };

        field.addActionListener(e -> commit.run());
        field.addFocusListener(new java.awt.event.FocusAdapter() {
            @Override
            public void focusLost(java.awt.event.FocusEvent e) {
                commit.run();
            }
        });
    }

    private void installMaximizeTiling(JInternalFrame frame) {
        frame.addPropertyChangeListener(JInternalFrame.IS_MAXIMUM_PROPERTY, new PropertyChangeListener() {
            @Override
            public void propertyChange(PropertyChangeEvent evt) {
                if (Boolean.TRUE.equals(evt.getNewValue())) {
                    tileFrames();
                }
            }
        });
    }

    private EditorTabPanel getCurrentPanel() {
        JInternalFrame frame = desktopPane.getSelectedFrame();
        if (frame != null && frame.getContentPane().getComponentCount() > 0) {
            Component comp = frame.getContentPane().getComponent(0);
            if (comp instanceof EditorTabPanel panel) {
                return panel;
            }
        }
        return null;
    }

    private void saveAll() {
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            if (frame.getContentPane().getComponentCount() > 0 && frame.getContentPane().getComponent(0) instanceof EditorTabPanel panel) {
                panel.saveNow();
            }
        }
    }

    private void openNoteFromDb() {
        List<Note> notes = noteRepository.listAll();
        if (notes.isEmpty()) {
            JOptionPane.showMessageDialog(this, "暂无笔记，请先新建");
            return;
        }
        Note selected = (Note) JOptionPane.showInputDialog(this, "选择要打开的笔记", "打开笔记",
                JOptionPane.PLAIN_MESSAGE, null, notes.toArray(), notes.get(0));
        if (selected != null) {
            addFrame(selected);
        }
    }

    private void renameFrame(EditorTabPanel panel, String title) {
        for (JInternalFrame frame : desktopPane.getAllFrames()) {
            if (frame.getContentPane().getComponentCount() > 0 && frame.getContentPane().getComponent(0) == panel) {
                frame.setTitle(title);
                break;
            }
        }
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
}
