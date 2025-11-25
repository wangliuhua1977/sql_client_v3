package tools.sqlclient.ui;

import tools.sqlclient.exec.SqlExecResult;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;

/**
 * 通用结果面板，展示单条 SQL 执行的结果集。
 */
public class QueryResultPanel extends JPanel {
    public QueryResultPanel(SqlExecResult result, String titleHint) {
        super(new BorderLayout());
        setBorder(new TitledBorder("结果集"));
        setToolTipText(titleHint);

        JPanel header = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        JLabel countLabel = new JLabel("记录数 " + result.getRowsCount() + " 条");
        header.add(countLabel);
        add(header, BorderLayout.NORTH);

        DefaultTableModel model = new DefaultTableModel();
        result.getColumns().forEach(model::addColumn);
        for (var row : result.getRows()) {
            model.addRow(row.toArray());
        }
        JTable table = new JTable(model);
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

        DefaultTableCellRenderer renderer = new DefaultTableCellRenderer() {
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
        for (int i = 0; i < table.getColumnModel().getColumnCount(); i++) {
            table.getColumnModel().getColumn(i).setCellRenderer(renderer);
        }

        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.getViewport().setBackground(Color.WHITE);
        add(scrollPane, BorderLayout.CENTER);
    }
}
