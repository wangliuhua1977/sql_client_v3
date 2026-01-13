package tools.sqlclient.ui.findreplace;

import javax.swing.*;
import java.awt.*;

public class FindReplacePanel extends JPanel {
    private final JTextField searchField = new JTextField(18);
    private final JTextField replaceField = new JTextField(18);
    private final JButton prevButton = new JButton("上一处");
    private final JButton nextButton = new JButton("下一处");
    private final JButton replaceButton = new JButton("替换");
    private final JButton replaceFindButton = new JButton("替换并查找");
    private final JButton replaceAllButton = new JButton("全部替换");
    private final JCheckBox caseCheck = new JCheckBox("区分大小写");
    private final JCheckBox wholeCheck = new JCheckBox("全词匹配");
    private final JCheckBox regexCheck = new JCheckBox("正则");
    private final JCheckBox wrapCheck = new JCheckBox("循环查找");
    private final JLabel statusLabel = new JLabel(" ");
    private final JButton closeButton = new JButton("×");
    private final JToggleButton replaceToggle = new JToggleButton("替换");
    private final JPanel replaceRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 4));

    public FindReplacePanel() {
        setLayout(new BorderLayout());
        setBorder(BorderFactory.createMatteBorder(1, 0, 0, 0, new Color(220, 220, 220)));
        wrapCheck.setSelected(true);

        JPanel searchRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 4));
        searchRow.add(new JLabel("查找:"));
        searchRow.add(searchField);
        searchRow.add(prevButton);
        searchRow.add(nextButton);
        searchRow.add(replaceToggle);
        searchRow.add(caseCheck);
        searchRow.add(wholeCheck);
        searchRow.add(regexCheck);
        searchRow.add(wrapCheck);
        searchRow.add(statusLabel);
        closeButton.setMargin(new Insets(0, 6, 0, 6));
        closeButton.setFocusable(false);
        searchRow.add(closeButton);

        replaceRow.add(new JLabel("替换为:"));
        replaceRow.add(replaceField);
        replaceRow.add(replaceButton);
        replaceRow.add(replaceFindButton);
        replaceRow.add(replaceAllButton);
        replaceRow.setVisible(false);

        JPanel rows = new JPanel();
        rows.setLayout(new BoxLayout(rows, BoxLayout.Y_AXIS));
        rows.add(searchRow);
        rows.add(replaceRow);

        add(rows, BorderLayout.CENTER);

        replaceToggle.addActionListener(e -> setReplaceVisible(replaceToggle.isSelected()));
    }

    public JTextField getSearchField() {
        return searchField;
    }

    public JTextField getReplaceField() {
        return replaceField;
    }

    public JButton getPrevButton() {
        return prevButton;
    }

    public JButton getNextButton() {
        return nextButton;
    }

    public JButton getReplaceButton() {
        return replaceButton;
    }

    public JButton getReplaceFindButton() {
        return replaceFindButton;
    }

    public JButton getReplaceAllButton() {
        return replaceAllButton;
    }

    public JCheckBox getCaseCheck() {
        return caseCheck;
    }

    public JCheckBox getWholeCheck() {
        return wholeCheck;
    }

    public JCheckBox getRegexCheck() {
        return regexCheck;
    }

    public JCheckBox getWrapCheck() {
        return wrapCheck;
    }

    public JLabel getStatusLabel() {
        return statusLabel;
    }

    public JButton getCloseButton() {
        return closeButton;
    }

    public boolean isReplaceVisible() {
        return replaceRow.isVisible();
    }

    public void setReplaceVisible(boolean visible) {
        replaceRow.setVisible(visible);
        replaceToggle.setSelected(visible);
        revalidate();
        repaint();
    }

    public void focusSearchField() {
        searchField.requestFocusInWindow();
        searchField.selectAll();
    }

    public void setStatus(String text) {
        statusLabel.setText(text == null || text.isBlank() ? " " : text);
    }
}
