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
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
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
    private final JTextField searchField = new JTextField(18);
    private final JCheckBox fullTextBox = new JCheckBox("全文检索");
    private final JEditorPane preview = new JEditorPane("text/html", "");
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
        filter.add(new JLabel("检索:"));
        filter.add(searchField);
        filter.add(fullTextBox);
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
        searchField.getDocument().addDocumentListener(listener);
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
        table.getSelectionModel().addListSelectionListener(e -> updatePreview());
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2 && table.getSelectedRow() >= 0) {
                    int modelRow = table.convertRowIndexToModel(table.getSelectedRow());
                    opener.accept(tableModel.getNoteAt(modelRow));
                    dispose();
                }
            }
        });
        preview.setEditable(false);
        preview.setBorder(BorderFactory.createTitledBorder("内容预览"));
        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(table), new JScrollPane(preview));
        split.setDividerLocation(260);
        add(split, BorderLayout.CENTER);
    }

    private void loadNotes() {
        List<Note> notes = repository.search(searchField.getText(), fullTextBox.isSelected());
        tableModel.setData(notes);
        if (!notes.isEmpty()) {
            table.setRowSelectionInterval(0, 0);
        } else {
            preview.setText("<html><body><i>无匹配结果</i></body></html>");
        }
    }

    private void debounceSearch() {
        loadNotes();
    }

    private void updatePreview() {
        int viewRow = table.getSelectedRow();
        if (viewRow < 0) {
            preview.setText("<html><body><i>请选择笔记以预览内容</i></body></html>");
            return;
        }
        int modelRow = table.convertRowIndexToModel(viewRow);
        Note note = tableModel.getNoteAt(modelRow);
        String kw = searchField.getText();
        preview.setText(buildPreviewHtml(note.getContent(), kw));
        preview.setCaretPosition(0);
    }

    private String buildPreviewHtml(String content, String keyword) {
        if (content == null) content = "";
        if (keyword == null || keyword.isBlank()) {
            return "<html><body><pre>" + escape(content) + "</pre></body></html>";
        }
        String escaped = escape(content);
        String safeKw = escape(keyword.trim());
        try {
            return "<html><body><pre>" + escaped.replaceAll(java.util.regex.Pattern.quote(safeKw), "<mark>" + safeKw + "</mark>") + "</pre></body></html>";
        } catch (Exception ex) {
            return "<html><body><pre>" + escaped + "</pre></body></html>";
        }
    }

    private String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
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
