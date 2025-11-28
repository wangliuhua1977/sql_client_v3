package tools.sqlclient.ui;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableRowSorter;
import javax.swing.RowFilter;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 笔记管理：展示创建/修改时间、标签、星标，支持排序与全文检索。
 * 标题列前展示主窗口传入的“绘制头像图标”，可右键“更换图标”随机重抽。
 */
public class ManageNotesDialog extends JDialog {

    private final NoteRepository repository;
    private final Consumer<Note> opener;
    private final Function<Note, Icon> iconProvider;
    private final Function<Note, Icon> iconRefresher;

    private final NoteTableModel tableModel;
    private final JTable table;
    private final TableRowSorter<NoteTableModel> sorter;

    private final JTextField searchField = new JTextField(18);
    private final JCheckBox fullTextBox = new JCheckBox("全文搜索内容");

    private final JButton openButton = new JButton("打开");
    private final JButton trashButton = new JButton("移到垃圾箱");
    private final JButton restoreButton = new JButton("从垃圾箱还原");
    private final JButton emptyTrashButton = new JButton("清空垃圾箱");
    private final JButton closeButton = new JButton("关闭");

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public ManageNotesDialog(Frame owner,
                             NoteRepository repository,
                             Consumer<Note> opener,
                             Function<Note, Icon> iconProvider,
                             Function<Note, Icon> iconRefresher) {
        super(owner, "管理笔记", true);
        this.repository = repository;
        this.opener = opener;
        this.iconProvider = iconProvider;
        this.iconRefresher = iconRefresher;

        this.tableModel = new NoteTableModel(loadData());
        this.table = new JTable(tableModel);
        this.sorter = new TableRowSorter<>(tableModel);
        this.table.setRowSorter(sorter);

        initUI();
        setPreferredSize(new Dimension(880, 520));
        pack();
        setLocationRelativeTo(owner);
    }

    private List<Note> loadData() {
        List<Note> list = repository.listAll();
        return new ArrayList<>(list);
    }

    private void initUI() {
        setLayout(new BorderLayout(8, 8));

        // ==== 顶部搜索栏 ====
        JPanel searchPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        searchPanel.add(new JLabel("搜索："), gbc);

        gbc.gridx = 1;
        searchPanel.add(searchField, gbc);

        gbc.gridx = 2;
        searchPanel.add(fullTextBox, gbc);

        add(searchPanel, BorderLayout.NORTH);

        // 搜索实时过滤
        searchField.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                applyFilter();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                applyFilter();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                applyFilter();
            }
        });
        fullTextBox.addActionListener(e -> applyFilter());

        // ==== 中间表格 ====
        table.setFillsViewportHeight(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
        table.setRowHeight(24);

        // 列宽
        table.getColumnModel().getColumn(0).setPreferredWidth(40);   // 星标
        table.getColumnModel().getColumn(1).setPreferredWidth(260);  // 标题 + 图标
        table.getColumnModel().getColumn(2).setPreferredWidth(60);   // 数据库
        table.getColumnModel().getColumn(3).setPreferredWidth(160);  // 标签
        table.getColumnModel().getColumn(4).setPreferredWidth(120);  // 创建时间
        table.getColumnModel().getColumn(5).setPreferredWidth(120);  // 修改时间
        table.getColumnModel().getColumn(6).setPreferredWidth(80);   // 状态

        // 标题列：文本 + 图标
        table.getColumnModel().getColumn(1).setCellRenderer(
                new TitleWithIconRenderer(tableModel, iconProvider));

        // 时间列渲染
        DefaultTableCellRenderer timeRenderer = new DefaultTableCellRenderer() {
            @Override
            protected void setValue(Object value) {
                if (value instanceof Long l && l > 0) {
                    setText(TIME_FMT.format(Instant.ofEpochMilli(l)));
                } else {
                    setText("");
                }
            }
        };
        table.getColumnModel().getColumn(4).setCellRenderer(timeRenderer);
        table.getColumnModel().getColumn(5).setCellRenderer(timeRenderer);

        // 状态列居中
        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment(SwingConstants.CENTER);
        table.getColumnModel().getColumn(6).setCellRenderer(centerRenderer);

        // 鼠标行为：双击打开 + 右键菜单
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                // 双击左键打开
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) {
                    openSelectedNote();
                }
            }

            @Override
            public void mousePressed(MouseEvent e) {
                maybeShowPopup(e);
            }

            @Override
            public void mouseReleased(MouseEvent e) {
                maybeShowPopup(e);
            }

            private void maybeShowPopup(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    int row = table.rowAtPoint(e.getPoint());
                    if (row >= 0 && row < table.getRowCount()) {
                        table.setRowSelectionInterval(row, row);
                    }
                    showContextMenu(e.getX(), e.getY());
                }
            }
        });

        // 选中变化时更新按钮状态
        table.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                updateButtonState();
            }
        });

        add(new JScrollPane(table), BorderLayout.CENTER);

        // ==== 底部按钮 ====
        JPanel btnPanel = new JPanel();
        openButton.addActionListener(this::onOpen);
        trashButton.addActionListener(this::onTrash);
        restoreButton.addActionListener(this::onRestore);
        emptyTrashButton.addActionListener(this::onEmptyTrash);
        closeButton.addActionListener(e -> dispose());

        btnPanel.add(openButton);
        btnPanel.add(trashButton);
        btnPanel.add(restoreButton);
        btnPanel.add(emptyTrashButton);
        btnPanel.add(closeButton);

        add(btnPanel, BorderLayout.SOUTH);

        updateButtonState();
    }

    private void showContextMenu(int x, int y) {
        JPopupMenu menu = new JPopupMenu();

        JMenuItem openItem = new JMenuItem("打开");
        openItem.addActionListener(this::onOpen);
        menu.add(openItem);

        JMenuItem changeIconItem = new JMenuItem("更换图标");
        changeIconItem.addActionListener(e -> changeIconForSelectedNote());
        menu.add(changeIconItem);

        menu.show(table, x, y);
    }

    private void changeIconForSelectedNote() {
        if (iconRefresher == null) return;
        Note note = getSelectedNote();
        if (note == null) return;
        iconRefresher.apply(note);  // 让 MainFrame 重抽并持久化图标
        table.repaint();            // 重新绘制当前行图标
    }

    private void applyFilter() {
        String text = searchField.getText();
        if (text == null) text = "";
        final String query = text.trim().toLowerCase();
        if (query.isEmpty()) {
            sorter.setRowFilter(null);
            return;
        }
        final boolean fullText = fullTextBox.isSelected();
        sorter.setRowFilter(new RowFilter<NoteTableModel, Integer>() {
            @Override
            public boolean include(Entry<? extends NoteTableModel, ? extends Integer> entry) {
                int modelRow = entry.getIdentifier();
                Note note = tableModel.getNoteAt(modelRow);
                if (note == null) return false;
                String base = (safe(note.getTitle()) + " " + safe(note.getTags())).toLowerCase();
                if (base.contains(query)) return true;
                if (!fullText) return false;
                String content = safe(note.getContent()).toLowerCase();
                return content.contains(query);
            }
        });
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private void onOpen(ActionEvent e) {
        openSelectedNote();
    }

    private void onTrash(ActionEvent e) {
        Note note = getSelectedNote();
        if (note == null) return;
        if (note.isTrashed()) {
            JOptionPane.showMessageDialog(this, "该笔记已在垃圾箱中。");
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "确定将笔记《" + note.getTitle() + "》移到垃圾箱？",
                "确认", JOptionPane.YES_NO_OPTION);
        if (opt == JOptionPane.YES_OPTION) {
            repository.moveToTrash(note);
            reloadData();
        }
    }

    private void onRestore(ActionEvent e) {
        Note note = getSelectedNote();
        if (note == null) return;
        if (!note.isTrashed()) {
            JOptionPane.showMessageDialog(this, "该笔记不在垃圾箱中。");
            return;
        }
        repository.restore(note);
        reloadData();
    }

    private void onEmptyTrash(ActionEvent e) {
        int opt = JOptionPane.showConfirmDialog(this,
                "清空垃圾箱中的所有笔记？此操作不可恢复！",
                "危险操作确认", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            repository.emptyTrash();
            reloadData();
        }
    }

    private void reloadData() {
        tableModel.setData(loadData());
        applyFilter();
        updateButtonState();
    }

    private void openSelectedNote() {
        Note note = getSelectedNote();
        if (note == null || opener == null) return;
        opener.accept(note);
        dispose();
    }

    private Note getSelectedNote() {
        int viewRow = table.getSelectedRow();
        if (viewRow < 0) return null;
        int modelRow = table.convertRowIndexToModel(viewRow);
        return tableModel.getNoteAt(modelRow);
    }

    private void updateButtonState() {
        Note note = getSelectedNote();
        boolean has = note != null;
        openButton.setEnabled(has);
        trashButton.setEnabled(has && !note.isTrashed());
        restoreButton.setEnabled(has && note.isTrashed());
        emptyTrashButton.setEnabled(true);
    }

    // === 标题列渲染：文本 + 图标 ===
    private static class TitleWithIconRenderer extends DefaultTableCellRenderer {
        private final NoteTableModel model;
        private final Function<Note, Icon> iconProvider;

        TitleWithIconRenderer(NoteTableModel model, Function<Note, Icon> iconProvider) {
            this.model = model;
            this.iconProvider = iconProvider;
            setOpaque(true);
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value,
                                                       boolean isSelected, boolean hasFocus,
                                                       int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (!(c instanceof JLabel label)) {
                return c;
            }
            int modelRow = table.convertRowIndexToModel(row);
            Note note = model.getNoteAt(modelRow);
            Icon icon = null;
            if (note != null && iconProvider != null) {
                icon = iconProvider.apply(note);
            }
            label.setIcon(icon);
            label.setIconTextGap(6);
            label.setText(value == null ? "" : value.toString());

            if (note != null && note.isTrashed()) {
                label.setForeground(isSelected ? table.getSelectionForeground() : new java.awt.Color(150, 150, 150));
            } else {
                label.setForeground(isSelected ? table.getSelectionForeground() : table.getForeground());
            }
            return label;
        }
    }

    // === 表模型 ===
    private class NoteTableModel extends AbstractTableModel {
        private final String[] columns = {
                "★", "标题", "数据库", "标签", "创建时间", "修改时间", "状态"
        };
        private List<Note> data;

        NoteTableModel(List<Note> data) {
            this.data = data != null ? data : new ArrayList<>();
        }

        void setData(List<Note> newData) {
            this.data = newData != null ? newData : new ArrayList<>();
            fireTableDataChanged();
        }

        Note getNoteAt(int row) {
            if (row < 0 || row >= data.size()) return null;
            return data.get(row);
        }

        @Override
        public int getRowCount() {
            return data.size();
        }

        @Override
        public int getColumnCount() {
            return columns.length;
        }

        @Override
        public String getColumnName(int column) {
            return columns[column];
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return switch (columnIndex) {
                case 0 -> Boolean.class;
                case 4, 5 -> Long.class;
                default -> String.class;
            };
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return columnIndex == 0 || columnIndex == 3;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            Note note = data.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> note.isStarred();
                case 1 -> note.getTitle();
                case 2 -> note.getDatabaseType() == null ? "" : note.getDatabaseType().name();
                case 3 -> note.getTags();
                case 4 -> note.getCreatedAt();
                case 5 -> note.getUpdatedAt();
                case 6 -> note.isTrashed() ? "垃圾箱" : "正常";
                default -> null;
            };
        }

        @Override
        public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
            Note note = data.get(rowIndex);
            if (columnIndex == 0 && aValue instanceof Boolean b) {
                boolean starred = b;
                note.setStarred(starred);
                String tags = note.getTags();
                repository.updateMetadata(note, tags, starred);
            } else if (columnIndex == 3) {
                String tags = aValue == null ? "" : aValue.toString();
                note.setTags(tags);
                repository.updateMetadata(note, tags, note.isStarred());
            }
            fireTableRowsUpdated(rowIndex, rowIndex);
        }
    }
}
