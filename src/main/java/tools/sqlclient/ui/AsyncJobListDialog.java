package tools.sqlclient.ui;

import tools.sqlclient.exec.AsyncJobStatus;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.util.OperationLog;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;

/**
 * 查看与操作后端异步任务的列表窗口。
 */
public class AsyncJobListDialog extends JDialog {
    private final SqlExecutionService service;
    private final DefaultTableModel model = new DefaultTableModel();
    private final JLabel statusLabel = new JLabel("-");

    public AsyncJobListDialog(Frame owner, SqlExecutionService service) {
        super(owner, "异步任务列表", false);
        this.service = service;
        setSize(820, 420);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout());

        model.setColumnIdentifiers(new Object[]{"jobId", "状态", "进度", "标签", "SQL 摘要", "耗时(ms)"});
        JTable table = new JTable(model);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    showSelectedStatus();
                }
            }
        });
        add(new JScrollPane(table), BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        JButton refreshBtn = new JButton("刷新");
        refreshBtn.addActionListener(e -> refreshJobs());
        JButton cancelBtn = new JButton("取消任务");
        cancelBtn.addActionListener(e -> cancelSelected());
        actions.add(refreshBtn);
        actions.add(cancelBtn);
        actions.add(statusLabel);
        add(actions, BorderLayout.NORTH);

        refreshJobs();
    }

    private void refreshJobs() {
        statusLabel.setText("刷新中...");
        service.listJobs().thenAccept(list -> SwingUtilities.invokeLater(() -> applyJobs(list)))
                .exceptionally(ex -> {
                    SwingUtilities.invokeLater(() -> statusLabel.setText("刷新失败: " + ex.getMessage()));
                    return null;
                });
    }

    private void applyJobs(List<AsyncJobStatus> jobs) {
        model.setRowCount(0);
        if (jobs != null) {
            for (AsyncJobStatus job : jobs) {
                model.addRow(new Object[]{
                        job.getJobId(),
                        job.getStatus(),
                        job.getProgressPercent(),
                        job.getLabel(),
                        job.getSqlSummary(),
                        job.getElapsedMillis()
                });
            }
        }
        statusLabel.setText("共 " + (jobs == null ? 0 : jobs.size()) + " 条");
    }

    private void cancelSelected() {
        int row = ((JTable) ((JScrollPane) getContentPane().getComponent(0)).getViewport().getView()).getSelectedRow();
        if (row < 0) {
            statusLabel.setText("请选择任务");
            return;
        }
        String jobId = (String) model.getValueAt(row, 0);
        if (jobId == null || jobId.isBlank()) {
            statusLabel.setText("jobId 无效");
            return;
        }
        statusLabel.setText("取消中...");
        service.cancelJob(jobId, "用户手动取消").thenAccept(status -> SwingUtilities.invokeLater(() -> {
            statusLabel.setText("已请求取消: " + status.getStatus());
            OperationLog.log("手动取消任务 " + jobId + " -> " + status.getStatus());
            refreshJobs();
        })).exceptionally(ex -> {
            SwingUtilities.invokeLater(() -> statusLabel.setText("取消失败: " + ex.getMessage()));
            return null;
        });
    }

    private void showSelectedStatus() {
        int row = ((JTable) ((JScrollPane) getContentPane().getComponent(0)).getViewport().getView()).getSelectedRow();
        if (row < 0) {
            return;
        }
        String jobId = (String) model.getValueAt(row, 0);
        String status = String.valueOf(model.getValueAt(row, 1));
        statusLabel.setText("选中 " + jobId + " / " + status);
        OperationLog.log("查看任务 " + jobId + " 状态 " + status);
    }
}
