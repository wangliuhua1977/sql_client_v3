package tools.sqlclient.ui;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class TrashBinDialog extends JDialog {
    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final NoteRepository repository;
    private final Function<Note, Icon> iconProvider;
    private final NoteTableModel tableModel;
    private final JTable table;
    private final JButton restoreButton = new JButton("还原勾选");
    private final JButton deleteButton = new JButton("彻底删除");
    private final JButton emptyButton = new JButton("清空垃圾箱");
    private final JButton closeButton = new JButton("关闭");

    public TrashBinDialog(Frame owner, NoteRepository repository, Function<Note, Icon> iconProvider) {
        super(owner, "垃圾站管理", false);
        this.repository = repository;
        this.iconProvider = iconProvider;
        this.tableModel = new NoteTableModel(loadData());
        this.table = new JTable(tableModel);
        setModal(false);
        setPreferredSize(new Dimension(860, 520));
        setLocationRelativeTo(owner);
        setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        setLayout(new BorderLayout(8, 8));
        buildUI();
        pack();
    }

    public void open() {
        reload();
        setLocationRelativeTo(getOwner());
        setVisible(true);
        toFront();
    }

    private void buildUI() {
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.setBorder(new EmptyBorder(10, 10, 10, 10));

        configureTable();
        content.add(new JScrollPane(table), BorderLayout.CENTER);

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 8));
        buttons.add(restoreButton);
        buttons.add(deleteButton);
        buttons.add(emptyButton);
        buttons.add(closeButton);
        content.add(buttons, BorderLayout.SOUTH);

        restoreButton.addActionListener(this::onRestore);
        deleteButton.addActionListener(this::onDelete);
        emptyButton.addActionListener(this::onEmpty);
        closeButton.addActionListener(e -> setVisible(false));

        setContentPane(content);
        updateButtonState();
    }

    private void configureTable() {
        table.setFillsViewportHeight(true);
        table.setRowHeight(26);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.getColumnModel().getColumn(0).setPreferredWidth(46);
        table.getColumnModel().getColumn(1).setPreferredWidth(260);
        table.getColumnModel().getColumn(2).setPreferredWidth(90);
        table.getColumnModel().getColumn(3).setPreferredWidth(140);
        table.getColumnModel().getColumn(4).setPreferredWidth(140);

        table.getColumnModel().getColumn(1).setCellRenderer(new TitleRenderer(iconProvider));
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
        table.getColumnModel().getColumn(3).setCellRenderer(timeRenderer);
        table.getColumnModel().getColumn(4).setCellRenderer(timeRenderer);

        table.getModel().addTableModelListener(e -> updateButtonState());
        table.getSelectionModel().addListSelectionListener(e -> updateButtonState());
    }

    private void updateButtonState() {
        boolean hasChecked = !tableModel.getCheckedNotes().isEmpty();
        restoreButton.setEnabled(hasChecked);
        deleteButton.setEnabled(hasChecked);
        emptyButton.setEnabled(tableModel.getRowCount() > 0);
    }

    private List<Note> loadData() {
        return new ArrayList<>(repository.listByTrashed(true));
    }

    private void reload() {
        tableModel.setData(loadData());
        updateButtonState();
    }

    private void onRestore(ActionEvent e) {
        List<Note> selected = tableModel.getCheckedNotes();
        if (selected.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请先勾选要还原的笔记");
            return;
        }
        selected.forEach(repository::restore);
        reload();
    }

    private void onDelete(ActionEvent e) {
        List<Note> selected = tableModel.getCheckedNotes();
        if (selected.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请先勾选要删除的笔记");
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "彻底删除选中的 " + selected.size() + " 条笔记？此操作不可恢复！",
                "确认删除", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            selected.forEach(repository::deleteForever);
            reload();
        }
    }

    private void onEmpty(ActionEvent e) {
        if (tableModel.getRowCount() == 0) {
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "清空垃圾箱中的所有笔记？此操作不可恢复！",
                "清空垃圾箱", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            repository.emptyTrash();
            reload();
        }
    }

    private static class TitleRenderer extends DefaultTableCellRenderer {
        private final Function<Note, Icon> iconProvider;

        private TitleRenderer(Function<Note, Icon> iconProvider) {
            this.iconProvider = iconProvider;
            setOpaque(true);
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (c instanceof JLabel label) {
                Note note = ((NoteTableModel) table.getModel()).getNoteAt(table.convertRowIndexToModel(row));
                if (note != null && iconProvider != null) {
                    label.setIcon(iconProvider.apply(note));
                    label.setIconTextGap(6);
                }
                label.setText(value == null ? "" : value.toString());
            }
            return c;
        }
    }

    private static class NoteTableModel extends AbstractTableModel {
        private final String[] columns = {"选择", "标题", "数据库", "删除时间", "修改时间"};
        private List<RowData> data;

        private NoteTableModel(List<Note> notes) {
            setData(notes);
        }

        private void setData(List<Note> notes) {
            data = new ArrayList<>();
            if (notes != null) {
                for (Note note : notes) {
                    data.add(new RowData(note));
                }
            }
            fireTableDataChanged();
        }

        private List<Note> getCheckedNotes() {
            List<Note> list = new ArrayList<>();
            for (RowData row : data) {
                if (row.selected) {
                    list.add(row.note);
                }
            }
            return list;
        }

        private Note getNoteAt(int row) {
            if (row < 0 || row >= data.size()) return null;
            return data.get(row).note;
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
            if (columnIndex == 0) return Boolean.class;
            return String.class;
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return columnIndex == 0;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            RowData row = data.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> row.selected;
                case 1 -> row.note.getTitle();
                case 2 -> row.note.getDatabaseType() == null ? "" : row.note.getDatabaseType().name();
                case 3 -> row.note.getDeletedAt();
                case 4 -> row.note.getUpdatedAt();
                default -> null;
            };
        }

        @Override
        public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
            if (columnIndex == 0 && aValue instanceof Boolean b) {
                data.get(rowIndex).selected = b;
                fireTableRowsUpdated(rowIndex, rowIndex);
            }
        }

        private static class RowData {
            private final Note note;
            private boolean selected;

            private RowData(Note note) {
                this.note = note;
            }
        }
    }
}
