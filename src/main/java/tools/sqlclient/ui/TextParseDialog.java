package tools.sqlclient.ui;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文本解析窗口：左侧粘贴原始工单，右侧显示解析生成的 SQL。
 * 在原有右键菜单基础上新增“智能解析”项。
 */
public class TextParseDialog extends JDialog {
    private final JTextArea rawTextArea = new JTextArea();
    private final JTextArea parsedTextArea = new JTextArea();

    public TextParseDialog(Frame owner) {
        super(owner, "文本解析", false);
        setSize(900, 600);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout(8, 8));
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);

        rawTextArea.setLineWrap(true);
        rawTextArea.setWrapStyleWord(true);
        rawTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        rawTextArea.setComponentPopupMenu(createRawPopupMenu());

        parsedTextArea.setEditable(false);
        parsedTextArea.setLineWrap(true);
        parsedTextArea.setWrapStyleWord(true);
        parsedTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));

        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                wrapWithTitledScroll(rawTextArea, "原始文本"),
                wrapWithTitledScroll(parsedTextArea, "解析结果"));
        splitPane.setResizeWeight(0.6);
        splitPane.setContinuousLayout(true);
        splitPane.setDividerSize(10);

        JPanel content = new JPanel(new BorderLayout());
        content.setBorder(new EmptyBorder(10, 10, 10, 10));
        content.add(splitPane, BorderLayout.CENTER);
        setContentPane(content);
    }

    private JScrollPane wrapWithTitledScroll(JTextArea area, String title) {
        JScrollPane scroll = new JScrollPane(area);
        scroll.setBorder(BorderFactory.createTitledBorder(title));
        return scroll;
    }

    private JPopupMenu createRawPopupMenu() {
        JPopupMenu menu = new JPopupMenu();
        JMenuItem copy = new JMenuItem("复制");
        copy.addActionListener(e -> rawTextArea.copy());
        JMenuItem cut = new JMenuItem("剪切");
        cut.addActionListener(e -> rawTextArea.cut());
        JMenuItem paste = new JMenuItem("粘贴");
        paste.addActionListener(e -> rawTextArea.paste());
        JMenuItem selectAll = new JMenuItem("全选");
        selectAll.addActionListener(e -> rawTextArea.selectAll());

        JMenuItem smartParse = new JMenuItem("智能解析");
        smartParse.addActionListener(e -> performSmartParse());

        menu.add(copy);
        menu.add(cut);
        menu.add(paste);
        menu.add(selectAll);
        menu.addSeparator();
        menu.add(smartParse);
        return menu;
    }

    private void performSmartParse() {
        String selection = rawTextArea.getSelectedText();
        if (selection == null || selection.isBlank()) {
            JOptionPane.showMessageDialog(this, "请先选中要解析的工单文本。", "提示", JOptionPane.WARNING_MESSAGE);
            return;
        }

        String mobile = extractMobile(selection);
        if (mobile == null) {
            JOptionPane.showMessageDialog(this, "未找到手机号", "提示", JOptionPane.WARNING_MESSAGE);
            return;
        }

        String orderNo = extractOrderNo(selection);
        if (orderNo == null) {
            JOptionPane.showMessageDialog(this, "未找到单号", "提示", JOptionPane.WARNING_MESSAGE);
            return;
        }

        String reason = extractReason(selection);
        if (reason == null || reason.isBlank()) {
            JOptionPane.showMessageDialog(this, "未找到标题行", "提示", JOptionPane.WARNING_MESSAGE);
            return;
        }

        String param2 = orderNo + "," + reason;
        String sql = String.format(
                "select file_send('%s','%s','select * from  ' ,1,'qq123',6) ;",
                mobile,
                param2
        );

        if (!parsedTextArea.getText().isEmpty() && !parsedTextArea.getText().endsWith("\n")) {
            parsedTextArea.append("\n");
        }
        parsedTextArea.append(sql);
        parsedTextArea.append("\n");
    }

    private String extractMobile(String text) {
        Matcher matcher = Pattern.compile("1[3-9]\\d{9}").matcher(text);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    private String extractOrderNo(String text) {
        Pattern inline = Pattern.compile("单号[:：]\\s*([A-Za-z0-9_]+)");
        Pattern dataPattern = Pattern.compile("DATA[_A-Za-z0-9]+");
        boolean afterLabel = false;
        for (String line : text.split("\\r?\\n")) {
            Matcher inlineMatcher = inline.matcher(line);
            if (inlineMatcher.find()) {
                return inlineMatcher.group(1);
            }
            if (line.contains("单号")) {
                Matcher dataMatcher = dataPattern.matcher(line);
                if (dataMatcher.find()) {
                    return dataMatcher.group();
                }
                afterLabel = true;
                continue;
            }
            if (afterLabel) {
                Matcher dataMatcher = dataPattern.matcher(line);
                if (dataMatcher.find()) {
                    return dataMatcher.group();
                }
            }
        }
        return null;
    }

    private String extractReason(String text) {
        String[] lines = text.split("\\r?\\n");
        int titleIndex = -1;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].contains("标题")) {
                titleIndex = i;
                break;
            }
        }
        if (titleIndex < 0) {
            return null;
        }
        String titleLine = null;
        for (int i = titleIndex + 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                titleLine = lines[i].trim();
                break;
            }
        }
        if (titleLine == null) {
            return null;
        }

        String cleanTitle = titleLine.replaceAll("[,，]?\\s*使用期限[^天\\n]*天", "").trim();
        int idx = cleanTitle.indexOf('因');
        String reason;
        if (idx > 0) {
            reason = cleanTitle.substring(idx);
        } else {
            reason = cleanTitle;
        }
        reason = reason.replace('、', ',').replace('，', ',');
        reason = reason.replaceAll("\\s+", " ").trim();
        return reason;
    }
}
