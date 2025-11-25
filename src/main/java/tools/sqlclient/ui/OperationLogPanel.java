package tools.sqlclient.ui;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import java.awt.*;

/**
 * 主窗体右侧日志输出面板，用于显示控件变化、HTTPS 请求/响应等操作轨迹。
 */
public class OperationLogPanel extends JPanel {
    private final JTextArea area = new JTextArea();

    public OperationLogPanel() {
        super(new BorderLayout());
        setBorder(new TitledBorder("操作日志"));
        area.setEditable(false);
        area.setLineWrap(true);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        JScrollPane scroll = new JScrollPane(area);
        scroll.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
        add(scroll, BorderLayout.CENTER);
        JButton clear = new JButton("清空");
        clear.addActionListener(e -> area.setText(""));
        JPanel top = new JPanel(new FlowLayout(FlowLayout.RIGHT, 4, 4));
        top.add(clear);
        add(top, BorderLayout.NORTH);
        setPreferredSize(new Dimension(320, 200));
    }

    public void appendLine(String line) {
        if (line == null) return;
        area.append(line);
        area.append("\n");
        area.setCaretPosition(area.getDocument().getLength());
    }
}
