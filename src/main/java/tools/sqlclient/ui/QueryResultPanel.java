package tools.sqlclient.ui;

import tools.sqlclient.exec.AsyncJobStatus;
import tools.sqlclient.exec.ColumnDef;
import tools.sqlclient.exec.DatabaseErrorInfo;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlTopLevelClassifier;

import tools.sqlclient.ui.table.TableCopySupport;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellEditor;
import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Arrays;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 通用结果面板，展示单条 SQL 执行的结果集与异步进度。
 */
public class QueryResultPanel extends JPanel {
    private final JLabel countLabel = new JLabel("记录数 0 条");
    private final JLabel statusLabel = new JLabel("状态 -");
    private final JLabel elapsedLabel = new JLabel("耗时 -");
    private final JLabel jobLabel = new JLabel("");
    private final JLabel messageLabel = new JLabel();
    private final JProgressBar progressBar = new JProgressBar(0, 100);
    private final ColumnarTableModel model = new ColumnarTableModel();
    private final JTable table = new JTable(model);
    private final EditorDialogService editorDialogService = new EditorDialogService();
    private final DefaultTableModel infoModel = new DefaultTableModel(new Object[]{"字段", "值"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTable infoTable = new JTable(infoModel);
    private final JPanel cards = new JPanel(new CardLayout());
    private static final String CARD_TABLE = "table";
    private static final String CARD_INFO = "info";
    private final Color borderColor = new Color(228, 231, 236);
    private final DefaultTableCellRenderer stripedRenderer = new DefaultTableCellRenderer() {
        private final Color even = new Color(248, 251, 255);
        private final Color odd = new Color(244, 247, 252);
        private final Color selection = new Color(0xDCE6F5);
        private final Color selectionText = new Color(0x0F1F3A);

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            String text = value == null ? "" : value.toString();
            boolean multiline = text.contains("\n");
            if (multiline) {
                JTextArea area = new JTextArea(text);
                area.setLineWrap(true);
                area.setWrapStyleWord(true);
                area.setOpaque(true);
                area.setBorder(new EmptyBorder(4, 8, 4, 8));
                area.setFont(table.getFont());
                if (isSelected) {
                    area.setBackground(selection);
                    area.setForeground(selectionText);
                } else {
                    area.setBackground(row % 2 == 0 ? even : odd);
                    area.setForeground(new Color(0x1F2933));
                }
                int preferredHeight = Math.max(table.getRowHeight(), area.getPreferredSize().height + 4);
                if (table.getRowHeight(row) < preferredHeight) {
                    table.setRowHeight(row, preferredHeight);
                }
                return area;
            }

            JLabel label = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            label.setText(text);
            label.setHorizontalAlignment(SwingConstants.LEFT);
            label.setBorder(new EmptyBorder(0, 8, 0, 8));
            if (isSelected) {
                label.setBackground(selection);
                label.setForeground(selectionText);
            } else {
                label.setBackground(row % 2 == 0 ? even : odd);
                label.setForeground(new Color(0x1F2933));
            }
            return label;
        }
    };

    public QueryResultPanel(SqlExecResult result, String titleHint) {
        super(new BorderLayout());
        setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(1, 1, 1, 1, borderColor),
                new EmptyBorder(8, 8, 8, 8)));
        setToolTipText(titleHint);

        JPanel header = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        header.setBorder(new EmptyBorder(0, 0, 4, 0));
        header.add(countLabel);
        header.add(statusLabel);
        header.add(elapsedLabel);
        header.add(jobLabel);
        progressBar.setPreferredSize(new Dimension(160, 16));
        progressBar.setStringPainted(true);
        header.add(progressBar);
        add(header, BorderLayout.NORTH);

        JPanel messagePanel = new JPanel(new BorderLayout());
        messagePanel.setBorder(BorderFactory.createMatteBorder(1, 0, 0, 0, borderColor));
        messagePanel.add(messageLabel, BorderLayout.CENTER);
        add(messagePanel, BorderLayout.SOUTH);

        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        table.setShowGrid(false);
        table.setIntercellSpacing(new Dimension(0, 0));
        table.setRowHeight(24);
        table.setSelectionBackground(new Color(0xDCE6F5));
        table.setSelectionForeground(new Color(0x0F1F3A));
        table.setFillsViewportHeight(true);
        table.setBorder(BorderFactory.createLineBorder(borderColor));
        installCopySupport();
        installCellEditSupport();

        JTableHeader tableHeader = table.getTableHeader();
        Color headerBg = new Color(245, 247, 250);
        tableHeader.setBackground(headerBg);
        tableHeader.setFont(tableHeader.getFont().deriveFont(Font.BOLD));
        tableHeader.setOpaque(true);
        tableHeader.setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, borderColor));

        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scrollPane.getViewport().setBackground(Color.WHITE);
        scrollPane.setBorder(BorderFactory.createMatteBorder(1, 1, 1, 1, borderColor));

        infoTable.setFillsViewportHeight(true);
        infoTable.setRowHeight(24);
        infoTable.setShowGrid(false);
        infoTable.setIntercellSpacing(new Dimension(0, 0));
        infoTable.setSelectionBackground(new Color(0xDCE6F5));
        infoTable.setSelectionForeground(new Color(0x0F1F3A));
        infoTable.setDefaultRenderer(Object.class, stripedRenderer);
        JScrollPane infoScroll = new JScrollPane(infoTable);
        infoScroll.getViewport().setBackground(Color.WHITE);
        infoScroll.setBorder(BorderFactory.createMatteBorder(1, 1, 1, 1, borderColor));

        cards.add(scrollPane, CARD_TABLE);
        cards.add(infoScroll, CARD_INFO);
        add(cards, BorderLayout.CENTER);

        render(result);
    }




    public static QueryResultPanel pending(String sqlText) {
        List<String> cols = List.of("消息");
        List<List<String>> rows = List.of(List.of("任务已提交，等待执行..."));
        List<java.util.Map<String, String>> rowMaps = List.of(Map.of("消息", "任务已提交，等待执行..."));
        SqlExecResult res = SqlExecResult.builder(sqlText)
                .columns(cols)
                .rows(rows)
                .rowMaps(rowMaps)
                .rowCount(rows.size())
                .success(false)
                .message("任务已提交")
                .status("QUEUED")
                .progressPercent(0)
                .hasResultSet(true)
                .build();
        return new QueryResultPanel(res, sqlText);
    }

    public void updateProgress(AsyncJobStatus status) {
        if (status == null) {
            return;
        }
        setJobStatus(status.getJobId(), status.getStatus(), status.getProgressPercent(), status.getElapsedMillis());
        if (status.getMessage() != null) {
            messageLabel.setText(status.getMessage());
        }
        setBusy(!isTerminalStatus(status.getStatus()));
    }

    public void render(SqlExecResult result) {
        if (result == null) {
            return;
        }
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(() -> render(result));
            return;
        }
        setJobStatus(result.getJobId(), result.getStatus(), result.getProgressPercent(),
                result.getElapsedMillis() != null ? result.getElapsedMillis() : result.getDurationMillis());
        String msg = result.getMessage();
        if (msg != null && !msg.isBlank()) {
            messageLabel.setText(msg);
        } else if (result.getNote() != null && !result.getNote().isBlank()) {
            messageLabel.setText(result.getNote());
        }
        if (Boolean.TRUE.equals(result.getTruncated())) {
            String suffix = "结果已截断(" + (result.getMaxVisibleRows() != null ? result.getMaxVisibleRows() : "1000") + ")";
            if (messageLabel.getText() != null && !messageLabel.getText().isBlank()) {
                messageLabel.setText(messageLabel.getText() + " | " + suffix);
            } else {
                messageLabel.setText(suffix);
            }
        }
        boolean renderTable = shouldRenderResultSet(result);
        if (renderTable) {
            List<ColumnDef> defs = resolveColumns(result);
            List<List<String>> rows = result.getRows() != null ? result.getRows() : List.of();
            model.setData(defs, rows);
            applyColumnIdentifiers(defs);
            applyStripedRenderer();
            table.revalidate();
            table.repaint();
            resizeColumns();
            switchCard(CARD_TABLE);
        } else {
            renderInfo(result);
            switchCard(CARD_INFO);
        }
        int count = result.getTotalRows() != null ? result.getTotalRows() : result.getRowsCount();
        if (count <= 0) {
            countLabel.setText("返回 0 条记录");
        } else {
            countLabel.setText("记录数 " + count + " 条");
        }
        setBusy(!isTerminalStatus(result.getStatus()));
    }

    public void fitColumns() {
        resizeColumns();
    }

    public void focusTable() {
        table.requestFocusInWindow();
        if (table.getRowCount() > 0 && table.getSelectedRowCount() == 0) {
            table.setRowSelectionInterval(0, 0);
        }
    }

    public void resetColumns() {
        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        resizeColumns();
    }

    public int getVisibleRowCount() {
        return model.getRowCount();
    }

    public int getVisibleColumnCount() {
        return model.getColumnCount();
    }

    public void renderError(DatabaseErrorInfo errorInfo, String message, Integer statementIndex, String sqlFragment) {
        String displayMessage = (message == null || message.isBlank()) ? "执行失败" : message;
        List<ColumnDef> defs = List.of(
                new ColumnDef("field", "字段", "字段"),
                new ColumnDef("value", "内容", "内容")
        );
        List<List<String>> rows = new ArrayList<>();
        if (statementIndex != null) {
            rows.add(List.of("语句序号", "第 " + statementIndex + " 条"));
        }
        if (sqlFragment != null && !sqlFragment.isBlank()) {
            rows.add(List.of("SQL 片段", abbreviate(sqlFragment)));
        }
        rows.add(List.of("raw", displayMessage));
        if (errorInfo != null) {
            addIfPresent(rows, "message", errorInfo.getMessage());
            addIfPresent(rows, "sqlState", errorInfo.getSqlState());
            addIfPresent(rows, "position", errorInfo.getPosition());
            addIfPresent(rows, "detail", errorInfo.getDetail());
            addIfPresent(rows, "hint", errorInfo.getHint());
            addIfPresent(rows, "where", errorInfo.getWhere());
            addIfPresent(rows, "schema", errorInfo.getSchema());
            addIfPresent(rows, "table", errorInfo.getTable());
            addIfPresent(rows, "column", errorInfo.getColumn());
            addIfPresent(rows, "datatype", errorInfo.getDatatype());
            addIfPresent(rows, "constraint", errorInfo.getConstraint());
            addIfPresent(rows, "file", errorInfo.getFile());
            addIfPresent(rows, "line", errorInfo.getLine());
            addIfPresent(rows, "routine", errorInfo.getRoutine());
        }
        model.setData(defs, rows);
        applyColumnIdentifiers(defs);
        messageLabel.setText(displayMessage);
        setJobStatus(null, "FAILED", 100, null);
        applyStripedRenderer();
        table.revalidate();
        table.repaint();
        resizeColumns();
        setBusy(false);
    }

    private void addIfPresent(List<List<String>> rows, String field, Object value) {
        if (value == null) {
            return;
        }
        String text = String.valueOf(value);
        if (text.isBlank()) {
            return;
        }
        rows.add(List.of(field, text));
    }

    private String abbreviate(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.strip();
        return trimmed.length() > 200 ? trimmed.substring(0, 200) + "..." : trimmed;
    }

    private void applyStripedRenderer() {
        table.setDefaultRenderer(Object.class, stripedRenderer);
        for (int i = 0; i < table.getColumnModel().getColumnCount(); i++) {
            table.getColumnModel().getColumn(i).setCellRenderer(stripedRenderer);
        }
    }

    private void applyColumnIdentifiers(List<ColumnDef> defs) {
        for (int i = 0; i < Math.min(defs.size(), table.getColumnModel().getColumnCount()); i++) {
            var col = table.getColumnModel().getColumn(i);
            ColumnDef def = defs.get(i);
            col.setIdentifier(def.getId());
            col.setHeaderValue(def.getDisplayName());
        }
    }

    private void setJobStatus(String jobId, String status, Integer progressPercent, Long elapsedMillis) {
        if (status != null) {
            statusLabel.setText("状态 " + status);
        }
        if (jobId != null && !jobId.isBlank()) {
            jobLabel.setText("jobId=" + jobId);
        }
        if (progressPercent != null) {
            progressBar.setIndeterminate(false);
            progressBar.setValue(Math.max(0, Math.min(100, progressPercent)));
            progressBar.setString(progressPercent + "%");
        } else {
            progressBar.setIndeterminate(true);
            progressBar.setString("-");
        }
        if (elapsedMillis != null) {
            DecimalFormat df = new DecimalFormat("0.0");
            elapsedLabel.setText("耗时 " + df.format(elapsedMillis / 1000.0) + "s");
        }
    }

    private void resizeColumns() {
        // 计算列宽，确保内容不被压缩，并触发横向滚动条
        FontMetrics fm = table.getFontMetrics(table.getFont());
        int padding = 16; // 避免文字贴边
        int sampleRows = Math.min(table.getRowCount(), 200);
        for (int col = 0; col < table.getColumnCount(); col++) {
            int headerWidth = fm.stringWidth(table.getColumnName(col)) + padding;
            int cellWidth = headerWidth;
            for (int row = 0; row < sampleRows; row++) {
                Object value = table.getValueAt(row, col);
                if (value != null) {
                    cellWidth = Math.max(cellWidth, fm.stringWidth(value.toString()) + padding);
                }
            }
            table.getColumnModel().getColumn(col).setPreferredWidth(cellWidth);
        }
    }

    private void renderInfo(SqlExecResult result) {
        infoModel.setRowCount(0);
        addInfoRow("Status", result.getStatus());
        addInfoRow("Command Tag", result.getCommandTag());
        addInfoRow("Affected Rows", result.getRowsAffected() != null ? String.valueOf(result.getRowsAffected()) : "N/A");
        Long elapsed = result.getElapsedMillis() != null ? result.getElapsedMillis() : result.getDurationMillis();
        addInfoRow("Elapsed", elapsed != null ? elapsed + " ms" : "-");
        addInfoRow("Job Id", result.getJobId());
        addInfoRow("Message", result.getMessage());
        addInfoRow("Notices", joinList(result.getNotices()));
        addInfoRow("Warnings", joinList(result.getWarnings()));
    }

    private void addInfoRow(String key, String value) {
        infoModel.addRow(new Object[]{key, value == null || value.isBlank() ? "-" : value});
    }

    private String joinList(List<String> list) {
        if (list == null || list.isEmpty()) {
            return "-";
        }
        List<String> cleaned = list.stream()
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .toList();
        if (cleaned.isEmpty()) {
            return "-";
        }
        return String.join("; ", cleaned);
    }

    private void switchCard(String card) {
        CardLayout cl = (CardLayout) cards.getLayout();
        cl.show(cards, card);
    }

    private boolean shouldRenderResultSet(SqlExecResult result) {
        if (Boolean.TRUE.equals(result.getHasResultSet())) {
            return true;
        }
        if (Boolean.FALSE.equals(result.getHasResultSet())) {
            return false;
        }
        return SqlTopLevelClassifier.classify(result.getSql()) == SqlTopLevelClassifier.TopLevelType.RESULT_SET;
    }

    private List<ColumnDef> resolveColumns(SqlExecResult result) {
        if (result.getColumnDefs() != null && !result.getColumnDefs().isEmpty()) {
            return result.getColumnDefs();
        }
        List<ColumnDef> defs = new ArrayList<>();
        List<String> columns = result.getColumns() != null ? result.getColumns() : List.of();
        int seq = 1;
        for (String col : columns) {
            String display = col == null ? "" : col;
            defs.add(new ColumnDef(display + "#" + seq, display, display));
            seq++;
        }
        return defs;
    }

    public void setBusy(boolean busy) {
        SwingUtilities.invokeLater(() -> {
            progressBar.setVisible(busy);
            progressBar.setIndeterminate(busy);
            if (!busy) {
                progressBar.setValue(0);
                progressBar.setString("-");
            }
        });
    }

    private boolean isTerminalStatus(String status) {
        if (status == null) {
            return true;
        }
        return switch (status.toUpperCase()) {
            case "SUCCEEDED", "FAILED", "CANCELLED", "FINISHED", "COMPLETED", "ERROR" -> true;
            default -> false;
        };
    }

    private void installCopySupport() {
        table.setCellSelectionEnabled(true);
        table.setRowSelectionAllowed(true);
        table.setColumnSelectionAllowed(true);
        table.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        table.getColumnModel().getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

        InputMap inputMap = table.getInputMap(JComponent.WHEN_FOCUSED);
        ActionMap actionMap = table.getActionMap();
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_C, InputEvent.CTRL_DOWN_MASK), "copy-cells");
        actionMap.put("copy-cells", new AbstractAction() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent e) {
                copySelectedCells();
            }
        });

        JPopupMenu popupMenu = new JPopupMenu();
        JMenuItem copyCellsItem = new JMenuItem("复制选中单元格");
        JMenuItem copyColumnsItem = new JMenuItem("复制选中列内容");
        JMenuItem copyColumnNamesItem = new JMenuItem("复制选中列名（逗号分隔）");

        copyCellsItem.addActionListener(e -> copySelectedCells());
        copyColumnsItem.addActionListener(e -> copySelectedColumns());
        copyColumnNamesItem.addActionListener(e -> copySelectedColumnNames());

        popupMenu.add(copyCellsItem);
        popupMenu.add(copyColumnsItem);
        popupMenu.add(copyColumnNamesItem);

        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                maybeShowMenu(e);
            }

            @Override
            public void mouseReleased(MouseEvent e) {
                maybeShowMenu(e);
            }

            private void maybeShowMenu(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    ensureLeadSelection(e);
                    boolean hasData = table.getRowCount() > 0 && table.getColumnCount() > 0;
                    boolean hasCellSelection = hasData && table.getSelectedRowCount() > 0 && table.getSelectedColumnCount() > 0;
                    boolean hasColumns = hasData && !resolveTargetColumns().isEmpty();
                    copyCellsItem.setEnabled(hasCellSelection);
                    copyColumnsItem.setEnabled(hasColumns);
                    copyColumnNamesItem.setEnabled(hasColumns);
                    popupMenu.show(table, e.getX(), e.getY());
                }
            }

            private void ensureLeadSelection(MouseEvent e) {
                int row = table.rowAtPoint(e.getPoint());
                int col = table.columnAtPoint(e.getPoint());
                if (row >= 0 && col >= 0 && !table.isCellSelected(row, col)) {
                    table.changeSelection(row, col, false, false);
                }
            }
        });

        table.setComponentPopupMenu(popupMenu);
    }

    private void installCellEditSupport() {
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() != 2 || !SwingUtilities.isLeftMouseButton(e)) {
                    return;
                }
                int viewRow = table.rowAtPoint(e.getPoint());
                int viewCol = table.columnAtPoint(e.getPoint());
                if (viewRow < 0 || viewCol < 0) {
                    return;
                }
                TableCellEditor activeEditor = table.getCellEditor();
                if (activeEditor != null) {
                    activeEditor.stopCellEditing();
                }
                int modelRow = table.convertRowIndexToModel(viewRow);
                int modelCol = table.convertColumnIndexToModel(viewCol);
                Object value = model.getValueAt(modelRow, modelCol);
                String text = value == null ? "" : value.toString();
                String title = buildEditorTitle(viewRow, viewCol);
                Window owner = resolveOwnerWindow();
                EditorDialogService.Result result = editorDialogService.showDialog(owner, title, text);
                if (result != null && result.confirmed()) {
                    editorDialogService.apply(model, modelRow, modelCol, result.value());
                }
            }
        });
    }

    private Window resolveOwnerWindow() {
        Window owner = SwingUtilities.getWindowAncestor(this);
        if (owner != null) {
            return owner;
        }
        owner = KeyboardFocusManager.getCurrentKeyboardFocusManager().getActiveWindow();
        if (owner != null) {
            return owner;
        }
        return JOptionPane.getRootFrame();
    }

    private String buildEditorTitle(int viewRow, int viewCol) {
        String colName = table.getColumnName(viewCol);
        int displayRow = viewRow + 1;
        if (colName == null || colName.isBlank()) {
            return "编辑单元格 (row=" + displayRow + ")";
        }
        return "编辑单元格 (row=" + displayRow + ", col=" + colName + ")";
    }

    private void copySelectedCells() {
        String text = TableCopySupport.buildCellSelectionTsv(table);
        if (!text.isEmpty()) {
            TableCopySupport.copyToClipboard(text);
        }
    }


    private void copySelectedColumns() {
        java.util.List<Integer> cols = resolveTargetColumns();
        if (cols.isEmpty()) {
            return;
        }
        String text = TableCopySupport.buildSelectedColumnValuesTsv(table, cols);
        TableCopySupport.copyToClipboard(text);
    }

    private void copySelectedColumnNames() {
        java.util.List<Integer> cols = resolveTargetColumns();
        if (cols.isEmpty()) {
            return;
        }
        String text = TableCopySupport.buildSelectedColumnNamesCsv(table, cols);
        if (!text.isEmpty()) {
            TableCopySupport.copyToClipboard(text);
        }
    }

    private java.util.List<Integer> resolveTargetColumns() {
        int[] selected = table.getSelectedColumns();
        if (selected != null && selected.length > 0) {
            return Arrays.stream(selected).sorted().boxed().toList();
        }
        int lead = table.getSelectedColumn();
        if (lead < 0) {
            lead = table.getColumnModel().getSelectionModel().getLeadSelectionIndex();
        }
        if (lead >= 0) {
            return java.util.List.of(lead);
        }
        return java.util.List.of();
    }
}
