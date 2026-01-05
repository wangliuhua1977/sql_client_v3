package tools.sqlclient.ui;

import tools.sqlclient.pg.RoutineInfo;

import javax.swing.*;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 参数化运行函数/存储过程。
 */
public class RoutineRunDialog extends JDialog {
    public record Result(boolean confirmed, String sql, List<String> rawInputs) {}

    private Result result;

    public RoutineRunDialog(Frame owner, RoutineInfo info) {
        super(owner, "运行/调试 " + info.displayName(), true);
        setSize(520, 360);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout());

        List<String> args = parseArgs(info.fullArgs());
        JPanel form = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(4, 4, 4, 4);
        gbc.gridy = 0;
        gbc.gridx = 0;
        gbc.anchor = GridBagConstraints.WEST;

        JCheckBox nullWhenEmpty = new JCheckBox("空输入按 NULL 处理", true);
        List<JTextField> fields = new ArrayList<>();

        if (args.isEmpty()) {
            form.add(new JLabel("无参数"), gbc);
        } else {
            for (String arg : args) {
                JTextField field = new JTextField(24);
                fields.add(field);

                gbc.gridx = 0;
                gbc.weightx = 0;
                form.add(new JLabel(arg), gbc);

                gbc.gridx = 1;
                gbc.weightx = 1;
                gbc.fill = GridBagConstraints.HORIZONTAL;
                form.add(field, gbc);
                gbc.gridy++;
            }
        }

        JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.add(new JScrollPane(form), BorderLayout.CENTER);
        wrapper.add(nullWhenEmpty, BorderLayout.SOUTH);
        add(wrapper, BorderLayout.CENTER);

        JButton ok = new JButton("运行");
        ok.addActionListener(e -> {
            List<String> values = new ArrayList<>();
            for (JTextField field : fields) {
                String raw = field.getText();
                if (nullWhenEmpty.isSelected() && (raw == null || raw.isBlank())) {
                    values.add("NULL");
                } else {
                    values.add(raw == null ? "" : raw);
                }
            }
            String generatedSql = buildExecuteSql(info, values);
            result = new Result(true, generatedSql, values);
            dispose();
        });

        JButton cancel = new JButton("取消");
        cancel.addActionListener(e -> {
            result = new Result(false, null, List.of());
            dispose();
        });

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        actions.add(ok);
        actions.add(cancel);
        add(actions, BorderLayout.SOUTH);
    }

    public Result getResult() {
        return result == null ? new Result(false, null, List.of()) : result;
    }

    private List<String> parseArgs(String fullArgs) {
        if (fullArgs == null || fullArgs.isBlank()) {
            return List.of();
        }
        List<String> args = new ArrayList<>();
        for (String raw : fullArgs.split(",")) {
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) continue;
            args.add(trimmed);
        }
        return args;
    }

    private String buildExecuteSql(RoutineInfo info, List<String> values) {
        String joined = String.join(", ", values);
        String qualified = (info.schemaName() == null ? "leshan" : info.schemaName())
                + "." + info.objectName();
        if (info.isProcedure()) {
            return "CALL " + qualified + "(" + joined + ");";
        }
        boolean setof = info.resultType() != null && info.resultType().toLowerCase().startsWith("setof ");
        String select = setof ? "SELECT * FROM " : "SELECT ";
        return select + qualified + "(" + joined + ");";
    }
}
