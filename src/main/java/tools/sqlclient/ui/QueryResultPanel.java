package tools.sqlclient.ui;

import tools.sqlclient.exec.AsyncJobStatus;
import tools.sqlclient.exec.SqlExecResult;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.text.DecimalFormat;
import java.util.List;

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
    private final DefaultTableModel model = new DefaultTableModel();
    private final JTable table = new JTable(model);
    private final DefaultTableCellRenderer stripedRenderer = new DefaultTableCellRenderer() {
        private final Color even = new Color(250, 252, 255);
        private final Color odd = Color.WHITE;

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (!isSelected) {
                c.setBackground(row % 2 == 0 ? even : odd);
            }
            return c;
        }
    };

    public QueryResultPanel(SqlExecResult result, String titleHint) {
        super(new BorderLayout());
        setBorder(new TitledBorder("结果集"));
        setToolTipText(titleHint);

        JPanel header = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        header.add(countLabel);
        header.add(statusLabel);
        header.add(elapsedLabel);
        header.add(jobLabel);
        progressBar.setPreferredSize(new Dimension(160, 16));
        progressBar.setStringPainted(true);
        header.add(progressBar);
        add(header, BorderLayout.NORTH);

        JPanel messagePanel = new JPanel(new BorderLayout());
        messagePanel.setBorder(BorderFactory.createEmptyBorder(4, 8, 4, 8));
        messagePanel.add(messageLabel, BorderLayout.CENTER);
        add(messagePanel, BorderLayout.SOUTH);

        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        table.setShowGrid(true);
        table.setShowHorizontalLines(true);
        table.setShowVerticalLines(true);
        table.setGridColor(new Color(180, 186, 198));
        table.setIntercellSpacing(new Dimension(1, 1));
        table.setRowHeight(24);
        table.setFillsViewportHeight(true);
        table.setBorder(BorderFactory.createLineBorder(new Color(180, 186, 198)));

        Color headerBg = new Color(240, 244, 250);
        table.getTableHeader().setBackground(headerBg);
        table.getTableHeader().setOpaque(true);
        table.getTableHeader().setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, new Color(180, 186, 198)));

        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scrollPane.getViewport().setBackground(Color.WHITE);
        add(scrollPane, BorderLayout.CENTER);

        render(result);
    }

    public static QueryResultPanel pending(String sqlText) {
        List<String> cols = List.of("消息");
        List<List<String>> rows = List.of(List.of("任务已提交，等待执行..."));
        SqlExecResult res = new SqlExecResult(sqlText, cols, rows, rows.size(), false,
                "任务已提交", null, "QUEUED", 0, null, null, null, null, null, null, null, true,
                null, null, null, null, null);
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
    }

    public void render(SqlExecResult result) {
        if (result == null) {
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
        countLabel.setText("记录数 " + result.getRowsCount() + " 条");

        model.setColumnCount(0);
        model.setRowCount(0);
        if (result.getColumns() != null) {
            result.getColumns().forEach(model::addColumn);
        }
        if (result.getRows() != null) {
            for (var row : result.getRows()) {
                model.addRow(row.toArray());
            }
        }
        applyStripedRenderer();
        table.revalidate();
        table.repaint();
        resizeColumns();
    }

    public void fitColumns() {
        resizeColumns();
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

    public void renderError(String message) {
        model.setColumnCount(0);
        model.setRowCount(0);
        model.addColumn("错误");
        model.addRow(new Object[]{message});
        messageLabel.setText(message);
        setJobStatus(null, "FAILED", 100, null);
        applyStripedRenderer();
        table.revalidate();
        table.repaint();
        resizeColumns();
    }

    private void applyStripedRenderer() {
        for (int i = 0; i < table.getColumnModel().getColumnCount(); i++) {
            table.getColumnModel().getColumn(i).setCellRenderer(stripedRenderer);
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
}
