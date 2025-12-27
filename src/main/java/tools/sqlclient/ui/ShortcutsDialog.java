package tools.sqlclient.ui;

import javax.swing.*;
import java.awt.*;
import java.util.List;

public class ShortcutsDialog extends JDialog {
    public ShortcutsDialog(Frame owner, List<String> shortcuts) {
        super(owner, "快捷键", true);
        JTextArea area = new JTextArea();
        area.setEditable(false);
        area.setOpaque(false);
        area.setFont(new Font("Monospaced", Font.PLAIN, 12));
        StringBuilder sb = new StringBuilder();
        shortcuts.forEach(line -> sb.append(line).append(System.lineSeparator()));
        area.setText(sb.toString());

        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(UiStyle.panelPadding());
        panel.add(new JLabel("常用快捷键"), BorderLayout.NORTH);
        panel.add(new JScrollPane(area), BorderLayout.CENTER);

        setContentPane(panel);
        setSize(360, 320);
        setLocationRelativeTo(owner);
    }
}
