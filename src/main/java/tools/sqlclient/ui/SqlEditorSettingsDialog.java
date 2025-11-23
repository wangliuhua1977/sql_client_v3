package tools.sqlclient.ui;

import javax.swing.*;
import java.awt.*;

/**
 * SQL 编辑器设置对话框。
 */
public class SqlEditorSettingsDialog extends JDialog {
    private final JCheckBox convertFullWidthBox = new JCheckBox("自动将常见中文全角符号转为英文半角");
    private boolean confirmed = false;

    public SqlEditorSettingsDialog(Frame owner, boolean currentValue) {
        super(owner, "SQL 编辑器选项", true);
        setLayout(new BorderLayout());
        convertFullWidthBox.setSelected(currentValue);
        add(convertFullWidthBox, BorderLayout.CENTER);
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton ok = new JButton("确定");
        JButton cancel = new JButton("取消");
        ok.addActionListener(e -> { confirmed = true; setVisible(false); });
        cancel.addActionListener(e -> setVisible(false));
        buttons.add(ok);
        buttons.add(cancel);
        add(buttons, BorderLayout.SOUTH);
        pack();
        setLocationRelativeTo(owner);
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public boolean isConvertFullWidthEnabled() {
        return convertFullWidthBox.isSelected();
    }
}
