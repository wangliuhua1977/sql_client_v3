package tools.sqlclient.ui;

import tools.sqlclient.db.SqlHistoryRepository;
import tools.sqlclient.model.SqlHistoryEntry;
import tools.sqlclient.db.SqlSnippetRepository;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

public class ExecutionHistoryDialog extends JDialog {
    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final SqlHistoryRepository repository;
    private final SqlSnippetRepository snippetRepository;
    private final Consumer<String> insertHandler;
    private final HistoryTableModel tableModel = new HistoryTableModel();
    private final JTable historyTable = new JTable(tableModel);
    private final TableRowSorter<TableModel> sorter = new TableRowSorter<>(tableModel);
    private final JTextField keywordField = new JTextField();
    private final JTextField startDateField = new JTextField(10);
    private final JTextField endDateField = new JTextField(10);
    private final JButton deleteButton = new JButton("删除选中");
    private final JButton clearButton = new JButton("清空全部");
    private final JButton saveSnippetButton = new JButton("保存为片段");
    private final JButton closeButton = new JButton("关闭");
    private final JLabel feedbackLabel = new JLabel(" ");

    public ExecutionHistoryDialog(Frame owner, SqlHistoryRepository repository, SqlSnippetRepository snippetRepository,
                                  Consumer<String> insertHandler) {
        super(owner, "执行历史", false);
        this.repository = repository;
        this.snippetRepository = snippetRepository;
        this.insertHandler = insertHandler;
        setModal(false);
        setSize(900, 560);
        setLocationRelativeTo(owner);
        setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        setLayout(new BorderLayout(8, 8));
        buildUI();
        registerActions();
    }

    public void toggleVisible() {
        refreshList();
        if (isVisible() && isActive()) {
            setVisible(false);
            return;
        }
        setLocationRelativeTo(getOwner());
        setVisible(true);
        toFront();
        SwingUtilities.invokeLater(() -> {
            keywordField.requestFocusInWindow();
            if (historyTable.getRowCount() > 0) {
                historyTable.setRowSelectionInterval(0, 0);
            }
        });
    }

    private void buildUI() {
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.setBorder(new EmptyBorder(10, 10, 10, 10));

        JPanel filterPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        keywordField.setColumns(18);
        keywordField.putClientProperty("JTextField.placeholderText", "输入关键字筛选 SQL…");
        startDateField.setToolTipText("开始日期 yyyy-MM-dd");
        endDateField.setToolTipText("结束日期 yyyy-MM-dd");
        filterPanel.add(new JLabel("关键字:"));
        filterPanel.add(keywordField);
        filterPanel.add(new JLabel("开始时间:"));
        filterPanel.add(startDateField);
        filterPanel.add(new JLabel("结束时间:"));
        filterPanel.add(endDateField);
        content.add(filterPanel, BorderLayout.NORTH);

        historyTable.setRowSorter(sorter);
        historyTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        historyTable.setRowHeight(48);
        historyTable.setFillsViewportHeight(true);
        historyTable.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
        historyTable.setDefaultRenderer(String.class, new SqlPreviewRenderer());
        historyTable.setDefaultRenderer(Long.class, new TimeCellRenderer());
        sorter.setSortsOnUpdates(true);
        sorter.setComparator(1, Comparator.comparingLong(Long::longValue));
        sorter.setSortKeys(List.of(new RowSorter.SortKey(1, SortOrder.DESCENDING)));
        JScrollPane scrollPane = new JScrollPane(historyTable);
        content.add(scrollPane, BorderLayout.CENTER);

        JPanel bottom = new JPanel(new BorderLayout());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 6));
        buttons.add(saveSnippetButton);
        buttons.add(deleteButton);
        buttons.add(clearButton);
        buttons.add(closeButton);
        bottom.add(buttons, BorderLayout.EAST);
        feedbackLabel.setForeground(new Color(0x2d9cdb));
        feedbackLabel.setBorder(new EmptyBorder(6, 6, 6, 6));
        bottom.add(feedbackLabel, BorderLayout.WEST);
        content.add(bottom, BorderLayout.SOUTH);

        setContentPane(content);
    }

    private void registerActions() {
        DocumentListener filterListener = new DocumentListener() {
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
        };
        keywordField.getDocument().addDocumentListener(filterListener);
        startDateField.getDocument().addDocumentListener(filterListener);
        endDateField.getDocument().addDocumentListener(filterListener);

        historyTable.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_ENTER) {
                    insertSelection();
                } else if (e.getKeyCode() == KeyEvent.VK_DELETE) {
                    deleteSelection();
                }
            }
        });
        historyTable.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                if (e.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(e)) {
                    insertSelection();
                }
            }
        });

        deleteButton.addActionListener(e -> deleteSelection());
        clearButton.addActionListener(this::onClearAll);
        saveSnippetButton.addActionListener(e -> saveSelectionAsSnippet());
        closeButton.addActionListener(e -> setVisible(false));

        getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW)
                .put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "close_dialog");
        getRootPane().getActionMap().put("close_dialog", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setVisible(false);
            }
        });
    }

    private void refreshList() {
        List<SqlHistoryEntry> entries = repository.listAll();
        tableModel.setEntries(entries);
        applyFilter();
        feedbackLabel.setText(entries.isEmpty() ? "暂无历史记录" : "共 " + entries.size() + " 条历史记录");
    }

    private void insertSelection() {
        SqlHistoryEntry entry = getSelectedEntry();
        if (entry == null || insertHandler == null) {
            return;
        }
        insertHandler.accept(entry.getSqlText());
        feedbackLabel.setText("已插入到编辑器");
    }

    private void deleteSelection() {
        SqlHistoryEntry entry = getSelectedEntry();
        if (entry == null) {
            JOptionPane.showMessageDialog(this, "请先选择要删除的历史记录", "提示", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "确定删除该条历史记录？", "确认删除", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            repository.delete(entry.getId());
            refreshList();
        }
    }

    private void onClearAll(ActionEvent e) {
        if (tableModel.isEmpty()) {
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "清空全部执行历史？此操作不可恢复。",
                "确认清空", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            repository.clearAll();
            refreshList();
        }
    }

    private SqlHistoryEntry getSelectedEntry() {
        int viewRow = historyTable.getSelectedRow();
        if (viewRow < 0) {
            return null;
        }
        int modelRow = historyTable.convertRowIndexToModel(viewRow);
        return tableModel.getEntry(modelRow);
    }

    private void applyFilter() {
        feedbackLabel.setText(" ");
        String keyword = keywordField.getText() == null ? "" : keywordField.getText().trim().toLowerCase(Locale.ROOT);
        Long startMillis = parseDateMillis(startDateField, false);
        Long endMillis = parseDateMillis(endDateField, true);
        sorter.setRowFilter(new RowFilter<>() {
            @Override
            public boolean include(Entry<? extends TableModel, ? extends Integer> entry) {
                SqlHistoryEntry historyEntry = tableModel.getEntry(entry.getIdentifier());
                if (historyEntry == null) {
                    return false;
                }
                if (startMillis != null && historyEntry.getLastExecutedAt() < startMillis) {
                    return false;
                }
                if (endMillis != null && historyEntry.getLastExecutedAt() > endMillis) {
                    return false;
                }
                if (keyword.isEmpty()) {
                    return true;
                }
                String sqlText = historyEntry.getSqlText() == null ? "" : historyEntry.getSqlText().toLowerCase(Locale.ROOT);
                return sqlText.contains(keyword);
            }
        });
        if (historyTable.getRowCount() > 0) {
            historyTable.setRowSelectionInterval(0, 0);
        }
    }

    private Long parseDateMillis(JTextField field, boolean endOfDay) {
        String text = field.getText() == null ? "" : field.getText().trim();
        if (text.isEmpty()) {
            return null;
        }
        try {
            LocalDate date = LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
            LocalDateTime dateTime = endOfDay ? date.atTime(LocalTime.MAX) : date.atStartOfDay();
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException ex) {
            feedbackLabel.setText("日期格式需 yyyy-MM-dd");
            return null;
        }
    }

    private void saveSelectionAsSnippet() {
        SqlHistoryEntry entry = getSelectedEntry();
        if (entry == null) {
            JOptionPane.showMessageDialog(this, "请先选择要保存的历史记录", "提示", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        String title = JOptionPane.showInputDialog(this, "请输入片段标题", "保存为片段", JOptionPane.PLAIN_MESSAGE);
        if (title == null || title.trim().isEmpty()) {
            return;
        }
        snippetRepository.create(title.trim(), "", entry.getSqlText());
        feedbackLabel.setText("已保存到 SQL 快捷片段");
    }

    private static String escapeHtml(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static class HistoryTableModel extends AbstractTableModel {
        private final String[] columns = {"SQL 命令", "执行时间"};
        private List<SqlHistoryEntry> data = new ArrayList<>();

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
            return columnIndex == 1 ? Long.class : String.class;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            SqlHistoryEntry entry = data.get(rowIndex);
            if (columnIndex == 0) {
                return entry.getSqlText();
            }
            return entry.getLastExecutedAt();
        }

        public SqlHistoryEntry getEntry(int row) {
            if (row < 0 || row >= data.size()) {
                return null;
            }
            return data.get(row);
        }

        public void setEntries(List<SqlHistoryEntry> entries) {
            data = new ArrayList<>(entries);
            fireTableDataChanged();
        }

        public boolean isEmpty() {
            return data.isEmpty();
        }
    }

    private static class SqlPreviewRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (c instanceof JLabel label) {
                String text = value == null ? "" : value.toString();
                String preview = text.replace("\n", "  ");
                if (preview.length() > 200) {
                    preview = preview.substring(0, 200) + "...";
                }
                label.setText("<html><div style='width:640px;'>" + escapeHtml(preview) + "</div></html>");
                label.setFont(label.getFont().deriveFont(Font.PLAIN, 13f));
                if (!isSelected) {
                    label.setBackground(new Color(0xF7F9FB));
                }
            }
            return c;
        }
    }

    private static class TimeCellRenderer extends DefaultTableCellRenderer {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (c instanceof JLabel label && value instanceof Long time) {
                label.setText(TIME_FMT.format(Instant.ofEpochMilli(time)));
                label.setFont(label.getFont().deriveFont(Font.PLAIN, 13f));
            }
            return c;
        }
    }
}
