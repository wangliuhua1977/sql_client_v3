package tools.sqlclient.util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 简易双向链接解析器，识别 [[标记]]，点击后高亮并展示提示。
 */
public class LinkResolver {
    private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[(.+?)\\]\\]");

    public static void install(JTextArea area) {
        area.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    int pos = area.viewToModel(e.getPoint());
                    String text = area.getText();
                    Matcher matcher = LINK_PATTERN.matcher(text);
                    while (matcher.find()) {
                        if (pos >= matcher.start() && pos <= matcher.end()) {
                            String anchor = matcher.group(1);
                            JOptionPane.showMessageDialog(area, "跳转到锚点: " + anchor + "\n(示例：真实实现可在多标签间跳转)");
                            break;
                        }
                    }
                }
            }
        });
    }
}
