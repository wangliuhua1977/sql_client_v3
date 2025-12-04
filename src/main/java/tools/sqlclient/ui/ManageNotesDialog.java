package tools.sqlclient.ui;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
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
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 笔记管理：展示创建/修改时间、标签、星标，支持排序与全文检索。
 * 标题列前展示主窗口传入的“绘制头像图标”，可右键“更换图标”随机重抽。
 */
public class ManageNotesDialog extends JDialog {

    private final NoteRepository repository;
    private final BiConsumer<Note, SearchNavigation> opener;
    private final Function<Note, Icon> iconProvider;
    private final Function<Note, Icon> iconRefresher;

    private final NoteTableModel activeModel;
    private final NoteTableModel trashModel;
    private final JTable activeTable;
    private final JTable trashTable;
    private final TableRowSorter<NoteTableModel> activeSorter;
    private final TableRowSorter<NoteTableModel> trashSorter;
    private final JTabbedPane tabbedPane = new JTabbedPane();
    private final DefaultListModel<Note> backlinkModel = new DefaultListModel<>();
    private final JList<Note> backlinkList = new JList<>(backlinkModel);

    private final JTextField searchField = new JTextField(18);
    private final JCheckBox fullTextBox = new JCheckBox("全文搜索内容");

    private final JButton openButton = new JButton("打开");
    private final JButton trashButton = new JButton("移到垃圾箱");
    private final JButton restoreButton = new JButton("从垃圾箱还原");
    private final JButton emptyTrashButton = new JButton("清空垃圾箱");
    private final JButton deleteButton = new JButton("彻底删除");
    private final JButton searchButton = new JButton("全文搜索预览");
    private final JButton closeButton = new JButton("关闭");

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public ManageNotesDialog(Frame owner,
                             NoteRepository repository,
                             BiConsumer<Note, SearchNavigation> opener,
                             Function<Note, Icon> iconProvider,
                             Function<Note, Icon> iconRefresher) {
        super(owner, "管理笔记", true);
        this.repository = repository;
        this.opener = opener;
        this.iconProvider = iconProvider;
        this.iconRefresher = iconRefresher;

        this.activeModel = new NoteTableModel(loadData(false));
        this.trashModel = new NoteTableModel(loadData(true));
        this.activeTable = new JTable(activeModel);
        this.trashTable = new JTable(trashModel);
        this.activeSorter = new TableRowSorter<>(activeModel);
        this.trashSorter = new TableRowSorter<>(trashModel);
        this.activeTable.setRowSorter(activeSorter);
        this.trashTable.setRowSorter(trashSorter);

        initUI();
        setPreferredSize(new Dimension(880, 520));
        pack();
        setLocationRelativeTo(owner);
    }

    private List<Note> loadData(boolean trashed) {
        List<Note> list = repository.listByTrashed(trashed);
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

        gbc.gridx = 3;
        searchPanel.add(searchButton, gbc);

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
        searchButton.addActionListener(e -> openSearchDialog());
        searchField.addActionListener(e -> {
            if (fullTextBox.isSelected()) {
                openSearchDialog();
            }
        });

        // ==== 中间表格 ====
        configureTable(activeTable, activeModel);
        configureTable(trashTable, trashModel);

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
                tabbedPane, createBacklinkPanel());
        split.setResizeWeight(0.72);
        split.setDividerSize(8);
        add(split, BorderLayout.CENTER);

        tabbedPane.addTab("笔记", new JScrollPane(activeTable));
        tabbedPane.addTab("垃圾箱", new JScrollPane(trashTable));
        tabbedPane.addChangeListener(e -> {
            applyFilter();
            updateButtonState();
            refreshBacklinks();
        });

        // ==== 底部按钮 ====
        JPanel btnPanel = new JPanel();
        openButton.addActionListener(this::onOpen);
        trashButton.addActionListener(this::onTrash);
        restoreButton.addActionListener(this::onRestore);
        emptyTrashButton.addActionListener(this::onEmptyTrash);
        deleteButton.addActionListener(this::onDeleteForever);
        closeButton.addActionListener(e -> dispose());

        btnPanel.add(openButton);
        btnPanel.add(trashButton);
        btnPanel.add(restoreButton);
        btnPanel.add(deleteButton);
        btnPanel.add(emptyTrashButton);
        btnPanel.add(closeButton);

        add(btnPanel, BorderLayout.SOUTH);

        updateButtonState();
    }

    private void configureTable(JTable table, NoteTableModel model) {
        table.setFillsViewportHeight(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
        table.setRowHeight(24);

        table.getColumnModel().getColumn(0).setPreferredWidth(50);   // 勾选
        table.getColumnModel().getColumn(1).setPreferredWidth(40);   // 星标
        table.getColumnModel().getColumn(2).setPreferredWidth(260);  // 标题 + 图标
        table.getColumnModel().getColumn(3).setPreferredWidth(60);   // 数据库
        table.getColumnModel().getColumn(4).setPreferredWidth(160);  // 标签
        table.getColumnModel().getColumn(5).setPreferredWidth(120);  // 创建时间
        table.getColumnModel().getColumn(6).setPreferredWidth(120);  // 修改时间
        table.getColumnModel().getColumn(7).setPreferredWidth(80);   // 状态

        table.getColumnModel().getColumn(2).setCellRenderer(
                new TitleWithIconRenderer(model, iconProvider));

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
        table.getColumnModel().getColumn(5).setCellRenderer(timeRenderer);
        table.getColumnModel().getColumn(6).setCellRenderer(timeRenderer);

        DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
        centerRenderer.setHorizontalAlignment(SwingConstants.CENTER);
        table.getColumnModel().getColumn(7).setCellRenderer(centerRenderer);

        table.getModel().addTableModelListener(e -> updateButtonState());

        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
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

        table.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting() && table.isShowing()) {
                updateButtonState();
                refreshBacklinks();
            }
        });
    }

    private void showContextMenu(int x, int y) {
        JPopupMenu menu = new JPopupMenu();

        JMenuItem openItem = new JMenuItem("打开");
        openItem.addActionListener(this::onOpen);
        menu.add(openItem);

        JMenuItem changeIconItem = new JMenuItem("更换图标");
        changeIconItem.addActionListener(e -> changeIconForSelectedNote());
        menu.add(changeIconItem);

        JTable table = currentTable();
        if (table != null) {
            menu.show(table, x, y);
        }
    }

    private void changeIconForSelectedNote() {
        if (iconRefresher == null) return;
        Note note = getSelectedNote();
        if (note == null) return;
        iconRefresher.apply(note);  // 让 MainFrame 重抽并持久化图标
        JTable table = currentTable();
        if (table != null) {
            table.repaint();            // 重新绘制当前行图标
        }
    }

    private void applyFilter() {
        String text = searchField.getText();
        if (text == null) text = "";
        final String query = text.trim().toLowerCase();
        TableRowSorter<NoteTableModel> sorter = currentSorter();
        NoteTableModel model = currentModel();
        if (sorter == null || model == null) return;
        if (query.isEmpty()) {
            sorter.setRowFilter(null);
            return;
        }
        final boolean fullText = fullTextBox.isSelected();
        sorter.setRowFilter(new RowFilter<NoteTableModel, Integer>() {
            @Override
            public boolean include(Entry<? extends NoteTableModel, ? extends Integer> entry) {
                int modelRow = entry.getIdentifier();
                Note note = model.getNoteAt(modelRow);
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

    private void openSearchDialog() {
        String keyword = searchField.getText();
        if (keyword == null || keyword.isBlank()) {
            JOptionPane.showMessageDialog(this, "请输入要搜索的内容");
            return;
        }
        if (!fullTextBox.isSelected()) {
            applyFilter();
            return;
        }
        String normalized = keyword.trim();
        List<SearchHit> hits = collectHits(normalized, isTrashTab());
        if (hits.isEmpty()) {
            JOptionPane.showMessageDialog(this, "未在" + (isTrashTab() ? "垃圾箱" : "笔记") + "中找到匹配项。");
            return;
        }

        JDialog dialog = new JDialog(this, "全文搜索结果", true);
        SearchHitTableModel model = new SearchHitTableModel(hits);
        JTable table = new JTable(model);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
        table.setRowHeight(26);
        table.getColumnModel().getColumn(0).setPreferredWidth(220);
        table.getColumnModel().getColumn(1).setPreferredWidth(80);
        table.getColumnModel().getColumn(2).setPreferredWidth(480);
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) {
                    int row = table.getSelectedRow();
                    if (row >= 0) {
                        SearchHit hit = model.getHit(table.convertRowIndexToModel(row));
                        if (hit != null && opener != null) {
                            opener.accept(hit.note(), new SearchNavigation(hit.offset(), normalized));
                            dialog.dispose();
                        }
                    }
                }
            }
        });
        dialog.setLayout(new BorderLayout());
        dialog.add(new JScrollPane(table), BorderLayout.CENTER);
        dialog.setSize(840, 420);
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private List<SearchHit> collectHits(String keyword, boolean trashOnly) {
        List<SearchHit> hits = new ArrayList<>();
        List<Note> notes = repository.search(keyword, true, trashOnly);
        for (Note n : notes) {
            if (trashOnly && !n.isTrashed()) continue;
            if (!trashOnly && n.isTrashed()) continue;
            for (Integer pos : findMatches(n.getContent(), keyword)) {
                hits.add(new SearchHit(n, pos, buildPreview(n.getContent(), pos, keyword.length())));
            }
        }
        return hits;
    }

    private List<Integer> findMatches(String content, String keyword) {
        List<Integer> list = new ArrayList<>();
        if (content == null || keyword == null || keyword.isBlank()) return list;
        String lower = content.toLowerCase();
        String target = keyword.toLowerCase();
        int idx = lower.indexOf(target);
        while (idx >= 0) {
            list.add(idx);
            idx = lower.indexOf(target, idx + target.length());
        }
        return list;
    }

    private String buildPreview(String content, int pos, int len) {
        if (content == null) return "";
        int start = Math.max(0, pos - 24);
        int end = Math.min(content.length(), pos + len + 24);
        String snippet = content.substring(start, end);
        return snippet.replaceAll("\n", " ");
    }

    private JTable currentTable() {
        return isTrashTab() ? trashTable : activeTable;
    }

    private NoteTableModel currentModel() {
        return isTrashTab() ? trashModel : activeModel;
    }

    private TableRowSorter<NoteTableModel> currentSorter() {
        return isTrashTab() ? trashSorter : activeSorter;
    }

    private boolean isTrashTab() {
        return tabbedPane.getSelectedIndex() == 1;
    }

    private void onOpen(ActionEvent e) {
        openSelectedNote();
    }

    private void onTrash(ActionEvent e) {
        if (isTrashTab()) return;
        List<Note> checked = activeModel.getCheckedNotes();
        if (checked.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请先勾选要移动的笔记");
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "确定将勾选的 " + checked.size() + " 条笔记移到垃圾箱？",
                "确认", JOptionPane.YES_NO_OPTION);
        if (opt == JOptionPane.YES_OPTION) {
            checked.forEach(repository::moveToTrash);
            reloadData();
        }
    }

    private void onRestore(ActionEvent e) {
        if (!isTrashTab()) return;
        List<Note> checked = trashModel.getCheckedNotes();
        if (checked.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请先勾选要还原的笔记");
            return;
        }
        checked.forEach(repository::restore);
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

    private void onDeleteForever(ActionEvent e) {
        if (!isTrashTab()) return;
        List<Note> checked = trashModel.getCheckedNotes();
        if (checked.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请先勾选要彻底删除的笔记");
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "确定彻底删除勾选的 " + checked.size() + " 条笔记？此操作无法恢复！",
                "警告", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            checked.forEach(repository::deleteForever);
            reloadData();
        }
    }

    private void reloadData() {
        activeModel.setData(loadData(false));
        trashModel.setData(loadData(true));
        applyFilter();
        updateButtonState();
        refreshBacklinks();
    }

    private void openSelectedNote() {
        Note note = getSelectedNote();
        if (note == null || opener == null) return;
        opener.accept(note, null);
        dispose();
    }

    private Note getSelectedNote() {
        JTable table = currentTable();
        NoteTableModel model = currentModel();
        if (table == null || model == null) return null;
        int viewRow = table.getSelectedRow();
        if (viewRow < 0) return null;
        int modelRow = table.convertRowIndexToModel(viewRow);
        return model.getNoteAt(modelRow);
    }

    private void updateButtonState() {
        Note note = getSelectedNote();
        NoteTableModel model = currentModel();
        boolean hasChecked = model != null && model.hasChecked();
        boolean trashTab = isTrashTab();
        openButton.setEnabled(note != null);
        trashButton.setEnabled(!trashTab && hasChecked);
        restoreButton.setEnabled(trashTab && hasChecked);
        deleteButton.setEnabled(trashTab && hasChecked);
        emptyTrashButton.setEnabled(trashTab && trashModel.getRowCount() > 0);
    }

    private JPanel createBacklinkPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("反向链接（指向当前选中笔记）"));
        backlinkList.setCellRenderer(new DefaultListCellRenderer() {
            @Override
            public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
                Component c = super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
                if (c instanceof JLabel label && value instanceof Note note) {
                    label.setText(note.getTitle() + "  ·  " + TIME_FMT.format(Instant.ofEpochMilli(note.getUpdatedAt())));
                    if (iconProvider != null) {
                        label.setIcon(iconProvider.apply(note));
                    }
                }
                return c;
            }
        });
        backlinkList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        backlinkList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) {
                    Note n = backlinkList.getSelectedValue();
                    if (n != null && opener != null) {
                        opener.accept(n, null);
                    }
                }
            }
        });
        panel.add(new JScrollPane(backlinkList), BorderLayout.CENTER);
        JLabel hint = new JLabel("双击链接的笔记即可打开");
        hint.setBorder(new EmptyBorder(4,8,4,8));
        panel.add(hint, BorderLayout.SOUTH);
        return panel;
    }

    private void refreshBacklinks() {
        backlinkModel.clear();
        Note note = getSelectedNote();
        if (note == null) return;
        try {
            repository.findBacklinks(note.getId()).forEach(backlinkModel::addElement);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "加载反向链接失败: " + ex.getMessage());
        }
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
                "选择", "★", "标题", "数据库", "标签", "创建时间", "修改时间", "状态"
        };
        private List<RowData> data;

        NoteTableModel(List<Note> data) {
            setData(data);
        }

        void setData(List<Note> newData) {
            this.data = new ArrayList<>();
            if (newData != null) {
                for (Note n : newData) {
                    this.data.add(new RowData(n));
                }
            }
            fireTableDataChanged();
        }

        List<Note> getCheckedNotes() {
            List<Note> list = new ArrayList<>();
            for (RowData row : data) {
                if (row.selected) {
                    list.add(row.note);
                }
            }
            return list;
        }

        boolean hasChecked() {
            return data.stream().anyMatch(r -> r.selected);
        }

        Note getNoteAt(int row) {
            if (row < 0 || row >= data.size()) return null;
            return data.get(row).note;
        }

        void clearChecks() {
            data.forEach(r -> r.selected = false);
            fireTableDataChanged();
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
                case 0, 1 -> Boolean.class;
                case 5, 6 -> Long.class;
                default -> String.class;
            };
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return columnIndex == 0 || columnIndex == 1 || columnIndex == 4;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            RowData row = data.get(rowIndex);
            Note note = row.note;
            return switch (columnIndex) {
                case 0 -> row.selected;
                case 1 -> note.isStarred();
                case 2 -> note.getTitle();
                case 3 -> note.getDatabaseType() == null ? "" : note.getDatabaseType().name();
                case 4 -> note.getTags();
                case 5 -> note.getCreatedAt();
                case 6 -> note.getUpdatedAt();
                case 7 -> note.isTrashed() ? "垃圾箱" : "正常";
                default -> null;
            };
        }

        @Override
        public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
            RowData row = data.get(rowIndex);
            Note note = row.note;
            if (columnIndex == 0 && aValue instanceof Boolean selected) {
                row.selected = selected;
            } else if (columnIndex == 1 && aValue instanceof Boolean b) {
                boolean starred = b;
                note.setStarred(starred);
                String tags = note.getTags();
                repository.updateMetadata(note, tags, starred);
            } else if (columnIndex == 4) {
                String tags = aValue == null ? "" : aValue.toString();
                note.setTags(tags);
                repository.updateMetadata(note, tags, note.isStarred());
            }
            fireTableRowsUpdated(rowIndex, rowIndex);
        }

        private class RowData {
            private final Note note;
            private boolean selected;

            private RowData(Note note) {
                this.note = note;
            }
        }
    }

    private static class SearchHitTableModel extends AbstractTableModel {
        private final List<SearchHit> hits;

        SearchHitTableModel(List<SearchHit> hits) {
            this.hits = hits != null ? hits : new ArrayList<>();
        }

        @Override
        public int getRowCount() {
            return hits.size();
        }

        @Override
        public int getColumnCount() {
            return 3;
        }

        @Override
        public String getColumnName(int column) {
            return switch (column) {
                case 0 -> "笔记";
                case 1 -> "位置";
                case 2 -> "预览";
                default -> "";
            };
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            SearchHit hit = hits.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> hit.note().getTitle();
                case 1 -> hit.offset();
                case 2 -> hit.preview();
                default -> null;
            };
        }

        SearchHit getHit(int row) {
            if (row < 0 || row >= hits.size()) return null;
            return hits.get(row);
        }
    }

    public record SearchNavigation(int offset, String keyword) {}
    private record SearchHit(Note note, int offset, String preview) {}
}
