package tools.sqlclient.ui;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.text.DefaultHighlighter;
import java.awt.*;

/**
 * 自适应日志面板，支持自动滚动、过滤与复制。
 */
public class LogPanel extends JPanel {
    private final JTextArea area = new JTextArea();
    private final JCheckBox autoScroll = new JCheckBox("自动滚动", true);
    private final JTextField filterField = new JTextField();

    public LogPanel() {
        super(new BorderLayout(6, 6));
        setBorder(new TitledBorder("操作日志"));
        area.setEditable(false);
        area.setLineWrap(true);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

        JScrollPane scroll = new JScrollPane(area);
        scroll.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
        add(scroll, BorderLayout.CENTER);

        JPanel top = new JPanel(new BorderLayout(4, 4));
        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 4));
        JButton clear = new JButton("清空");
        clear.addActionListener(e -> area.setText(""));
        JButton copy = new JButton("复制全部");
        copy.addActionListener(e -> area.selectAll());
        copy.addActionListener(e -> area.copy());
        actions.add(clear);
        actions.add(copy);
        actions.add(autoScroll);
        top.add(actions, BorderLayout.WEST);

        filterField.setToolTipText("输入关键字搜索日志");
        filterField.getDocument().addDocumentListener((SimpleDocumentListener) e -> highlightKeyword());
        top.add(filterField, BorderLayout.CENTER);
        add(top, BorderLayout.NORTH);
        setPreferredSize(new Dimension(420, 240));
    }

    public void appendLine(String line) {
        if (line == null) return;
        SwingUtilities.invokeLater(() -> {
            area.append(line);
            area.append("\n");
            if (autoScroll.isSelected()) {
                area.setCaretPosition(area.getDocument().getLength());
            }
            highlightKeyword();
        });
    }

    public void focusFilter() {
        SwingUtilities.invokeLater(filterField::requestFocusInWindow);
    }

    private void highlightKeyword() {
        SwingUtilities.invokeLater(() -> {
            area.getHighlighter().removeAllHighlights();
            String keyword = filterField.getText();
            if (keyword == null || keyword.isBlank()) {
                return;
            }
            String text = area.getText();
            int index = text.indexOf(keyword);
            if (index >= 0) {
                try {
                    area.getHighlighter().addHighlight(index, index + keyword.length(),
                            new DefaultHighlighter.DefaultHighlightPainter(new Color(255, 234, 138)));
                    area.setCaretPosition(index);
                } catch (Exception ignored) {
                }
            }
        });
    }
}
