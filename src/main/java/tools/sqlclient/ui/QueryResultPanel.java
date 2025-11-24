package tools.sqlclient.ui;

import tools.sqlclient.exec.SqlExecResult;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.*;

/**
 * 通用结果面板，展示单条 SQL 执行的结果集。
 */
public class QueryResultPanel extends JPanel {
    public QueryResultPanel(SqlExecResult result, String titleHint) {
        super(new BorderLayout());
        String title = "结果 (" + result.getRowsCount() + " 行)";
        setBorder(new TitledBorder(title));
        setToolTipText(titleHint);
        DefaultTableModel model = new DefaultTableModel();
        result.getColumns().forEach(model::addColumn);
        for (var row : result.getRows()) {
            model.addRow(row.toArray());
        }
        JTable table = new JTable(model);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        add(new JScrollPane(table), BorderLayout.CENTER);
    }
}
