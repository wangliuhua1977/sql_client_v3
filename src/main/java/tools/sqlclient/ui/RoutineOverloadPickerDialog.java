package tools.sqlclient.ui;

import tools.sqlclient.pg.RoutineInfo;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.List;

/**
 * 重载选择器：当仅有函数名而缺少 OID 时列出 identity args 让用户选择。
 */
public class RoutineOverloadPickerDialog extends JDialog {
    private RoutineInfo selected;

    public RoutineOverloadPickerDialog(Frame owner, List<RoutineInfo> routines) {
        super(owner, "选择重载", true);
        setSize(560, 320);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout());

        String[] columns = {"OID", "名称", "参数", "类型"};
        DefaultTableModel model = new DefaultTableModel(columns, 0) {
            @Override
            public boolean isCellEditable(int row, int column) {
                return false;
            }
        };
        JTable table = new JTable(model);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        routines.forEach(r -> model.addRow(new Object[]{
                r.oid(),
                r.schemaName() + "." + r.objectName(),
                r.identityArgs(),
                r.isFunction() ? "function" : "procedure"
        }));

        if (model.getRowCount() > 0) {
            table.setRowSelectionInterval(0, 0);
        }

        add(new JScrollPane(table), BorderLayout.CENTER);

        JButton ok = new JButton("确定");
        ok.addActionListener(e -> {
            int idx = table.getSelectedRow();
            if (idx >= 0) {
                selected = routines.get(idx);
                dispose();
            }
        });

        JButton cancel = new JButton("取消");
        cancel.addActionListener(e -> dispose());

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        actions.add(ok);
        actions.add(cancel);
        add(actions, BorderLayout.SOUTH);
    }

    public RoutineInfo getSelected() {
        return selected;
    }
}
