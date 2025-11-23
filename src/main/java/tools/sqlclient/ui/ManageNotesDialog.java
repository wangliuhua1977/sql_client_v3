package tools.sqlclient.ui;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 笔记管理：展示创建/修改时间、标签、星标，支持排序与全文检索。
 */
public class ManageNotesDialog extends JDialog {
    private final NoteRepository repository;
    private final Consumer<Note> opener;
    private final NoteTableModel tableModel;
    private final JTable table;
    private final JTextField keywordField = new JTextField(16);
    private final JTextField fullTextField = new JTextField(16);
    private final TableRowSorter<NoteTableModel> sorter;

    public ManageNotesDialog(Frame owner, NoteRepository repository, Consumer<Note> opener) {
        super(owner, "管理笔记", true);
        this.repository = repository;
        this.opener = opener;
        this.tableModel = new NoteTableModel(repository);
        this.table = new JTable(tableModel);
        this.sorter = new TableRowSorter<>(tableModel);
        setLayout(new BorderLayout());
        buildToolbar();
        buildTable();
        loadNotes();
        setSize(900, 520);
        setLocationRelativeTo(owner);
    }

    private void buildToolbar() {
        JPanel filter = new JPanel(new FlowLayout(FlowLayout.LEFT));
        filter.add(new JLabel("普通检索:"));
        filter.add(keywordField);
        filter.add(new JLabel("全文检索:"));
        filter.add(fullTextField);
        JButton search = new JButton("检索");
        search.addActionListener(e -> loadNotes());
        filter.add(search);
        JButton refresh = new JButton("刷新全部");
        refresh.addActionListener(e -> loadNotes());
        filter.add(refresh);
        add(filter, BorderLayout.NORTH);
        DocumentListener listener = new DocumentListener() {
            @Override public void insertUpdate(DocumentEvent e) { debounceSearch(); }
            @Override public void removeUpdate(DocumentEvent e) { debounceSearch(); }
            @Override public void changedUpdate(DocumentEvent e) { debounceSearch(); }
        };
        keywordField.getDocument().addDocumentListener(listener);
        fullTextField.getDocument().addDocumentListener(listener);
    }

    private void buildTable() {
        table.setRowSorter(sorter);
        sorter.setSortsOnUpdates(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoCreateRowSorter(true);
        table.setFillsViewportHeight(true);
        table.setDefaultRenderer(Long.class, new DefaultTableCellRenderer() {
            @Override
            protected void setValue(Object value) {
                if (value instanceof Long l) {
                    setText(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())
                            .format(Instant.ofEpochMilli(l)));
                } else {
                    super.setValue(value);
                }
            }
        });
        table.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                if (e.getClickCount() == 2 && table.getSelectedRow() >= 0) {
                    int modelRow = table.convertRowIndexToModel(table.getSelectedRow());
                    opener.accept(tableModel.getNoteAt(modelRow));
                    dispose();
                }
            }
        });
        add(new JScrollPane(table), BorderLayout.CENTER);
    }

    private void loadNotes() {
        List<Note> notes = repository.search(keywordField.getText(), fullTextField.getText());
        tableModel.setData(notes);
    }

    private void debounceSearch() {
        // 简单立即刷新，避免重复代码
        loadNotes();
    }

    private static class NoteTableModel extends AbstractTableModel {
        private final NoteRepository repository;
        private final String[] columns = {"★", "标题", "数据库", "标签", "创建时间", "最后修改时间"};
        private List<Note> data = new ArrayList<>();

        NoteTableModel(NoteRepository repository) {
            this.repository = repository;
        }

        public void setData(List<Note> notes) {
            this.data = new ArrayList<>(notes);
            fireTableDataChanged();
        }

        public Note getNoteAt(int row) {
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
            if (columnIndex == 0) return Boolean.class;
            if (columnIndex == 4 || columnIndex == 5) return Long.class;
            return String.class;
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
                case 2 -> note.getDatabaseType().name();
                case 3 -> note.getTags();
                case 4 -> note.getCreatedAt();
                case 5 -> note.getUpdatedAt();
                default -> "";
            };
        }

        @Override
        public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
            Note note = data.get(rowIndex);
            if (columnIndex == 0 && aValue instanceof Boolean) {
                boolean starred = (Boolean) aValue;
                repository.updateMetadata(note, note.getTags(), starred);
            } else if (columnIndex == 3) {
                String tags = aValue == null ? "" : aValue.toString();
                repository.updateMetadata(note, tags, note.isStarred());
            }
            fireTableRowsUpdated(rowIndex, rowIndex);
        }
    }
}
