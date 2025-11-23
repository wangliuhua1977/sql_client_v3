package tools.sqlclient.ui;

import tools.sqlclient.db.EditorStyleRepository;
import tools.sqlclient.model.EditorStyle;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.Optional;

/**
 * SQL 编辑器设置对话框，支持全角转换开关与多套主题保存。
 */
public class SqlEditorSettingsDialog extends JDialog {
    private final JCheckBox convertFullWidthBox = new JCheckBox("自动将常见中文全角符号转为英文半角");
    private final DefaultListModel<EditorStyle> listModel = new DefaultListModel<>();
    private final JList<EditorStyle> styleList = new JList<>(listModel);
    private final JTextField nameField = new JTextField(12);
    private final JSpinner fontSizeSpinner = new JSpinner(new SpinnerNumberModel(14, 8, 48, 1));
    private final JTextField bgField = new JTextField(7);
    private final JTextField fgField = new JTextField(7);
    private final JTextField selField = new JTextField(7);
    private final JTextField caretField = new JTextField(7);
    private final JTextField keywordField = new JTextField(7);
    private final JTextField stringField = new JTextField(7);
    private final JTextField commentField = new JTextField(7);
    private final EditorStyleRepository repository;
    private boolean confirmed = false;
    private EditorStyle selectedStyle;

    public SqlEditorSettingsDialog(Frame owner, boolean currentFullWidth, EditorStyleRepository repository, List<EditorStyle> styles, EditorStyle currentStyle) {
        super(owner, "SQL 编辑器选项", true);
        this.repository = repository;
        setLayout(new BorderLayout());
        convertFullWidthBox.setSelected(currentFullWidth);
        buildStyleList(styles, currentStyle);
        add(buildContent(), BorderLayout.CENTER);
        add(buildButtons(), BorderLayout.SOUTH);
        pack();
        setLocationRelativeTo(owner);
    }

    private JPanel buildContent() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.add(convertFullWidthBox, BorderLayout.NORTH);
        panel.add(buildStyleEditor(), BorderLayout.CENTER);
        return panel;
    }

    private void buildStyleList(List<EditorStyle> styles, EditorStyle currentStyle) {
        styles.forEach(listModel::addElement);
        styleList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        styleList.setCellRenderer((list, value, index, isSelected, cellHasFocus) -> {
            JLabel label = new JLabel(value.getName());
            if (isSelected) {
                label.setOpaque(true);
                label.setBackground(list.getSelectionBackground());
                label.setForeground(list.getSelectionForeground());
            }
            return label;
        });
        if (currentStyle != null) {
            selectStyleByName(currentStyle.getName());
        }
        styleList.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                EditorStyle style = styleList.getSelectedValue();
                if (style != null) {
                    loadStyleToForm(style);
                }
            }
        });
        if (styleList.getSelectedIndex() < 0 && !styles.isEmpty()) {
            styleList.setSelectedIndex(0);
            loadStyleToForm(styles.get(0));
        }
    }

    private JPanel buildStyleEditor() {
        JPanel editor = new JPanel(new BorderLayout());
        editor.setBorder(BorderFactory.createTitledBorder("主题设置"));
        editor.add(new JScrollPane(styleList), BorderLayout.WEST);

        JPanel form = new JPanel(new GridLayout(0, 2, 6, 6));
        form.add(new JLabel("样式名称"));
        form.add(nameField);
        form.add(new JLabel("字体大小"));
        form.add(fontSizeSpinner);
        form.add(new JLabel("背景色#"));
        form.add(bgField);
        form.add(new JLabel("前景色#"));
        form.add(fgField);
        form.add(new JLabel("选中色#"));
        form.add(selField);
        form.add(new JLabel("光标色#"));
        form.add(caretField);
        form.add(new JLabel("关键字色#"));
        form.add(keywordField);
        form.add(new JLabel("字符串色#"));
        form.add(stringField);
        form.add(new JLabel("注释色#"));
        form.add(commentField);

        JPanel right = new JPanel(new BorderLayout());
        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton save = new JButton("保存/更新样式");
        JButton newStyle = new JButton("新建样式");
        JButton setCurrent = new JButton("设为当前");
        save.addActionListener(e -> saveStyle());
        newStyle.addActionListener(e -> {
            nameField.setText("自定义" + (listModel.size() + 1));
            styleList.clearSelection();
        });
        setCurrent.addActionListener(e -> {
            EditorStyle style = captureStyleFromForm();
            if (style != null) {
                ensureSelected(style);
            }
        });
        actions.add(newStyle);
        actions.add(save);
        actions.add(setCurrent);
        right.add(actions, BorderLayout.SOUTH);
        right.add(form, BorderLayout.CENTER);

        editor.add(right, BorderLayout.CENTER);
        return editor;
    }

    private JPanel buildButtons() {
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton ok = new JButton("确定");
        JButton cancel = new JButton("取消");
        ok.addActionListener(e -> { confirmed = true; setVisible(false); });
        cancel.addActionListener(e -> setVisible(false));
        buttons.add(ok);
        buttons.add(cancel);
        return buttons;
    }

    private void loadStyleToForm(EditorStyle style) {
        selectedStyle = style;
        nameField.setText(style.getName());
        fontSizeSpinner.setValue(style.getFontSize());
        bgField.setText(stripHash(style.getBackground()));
        fgField.setText(stripHash(style.getForeground()));
        selField.setText(stripHash(style.getSelection()));
        caretField.setText(stripHash(style.getCaret()));
        keywordField.setText(stripHash(style.getKeyword()));
        stringField.setText(stripHash(style.getStringColor()));
        commentField.setText(stripHash(style.getCommentColor()));
    }

    private EditorStyle captureStyleFromForm() {
        String name = nameField.getText();
        if (name == null || name.isBlank()) {
            JOptionPane.showMessageDialog(this, "样式名称不能为空");
            return null;
        }
        try {
            int size = (Integer) fontSizeSpinner.getValue();
            return new EditorStyle(name.trim(), size,
                    color(bgField), color(fgField), color(selField), color(caretField),
                    color(keywordField), color(stringField), color(commentField));
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "颜色请输入 6 位十六进制，例如 FFFFFF");
            return null;
        }
    }

    private void saveStyle() {
        EditorStyle style = captureStyleFromForm();
        if (style == null) return;
        repository.upsert(style);
        ensureSelected(style);
    }

    private void ensureSelected(EditorStyle style) {
        boolean found = false;
        for (int i = 0; i < listModel.size(); i++) {
            if (listModel.get(i).getName().equals(style.getName())) {
                listModel.set(i, style);
                found = true;
                styleList.setSelectedIndex(i);
                break;
            }
        }
        if (!found) {
            listModel.addElement(style);
            styleList.setSelectedIndex(listModel.size() - 1);
        }
        selectedStyle = style;
    }

    private void selectStyleByName(String name) {
        for (int i = 0; i < listModel.size(); i++) {
            if (listModel.get(i).getName().equals(name)) {
                styleList.setSelectedIndex(i);
                loadStyleToForm(listModel.get(i));
                return;
            }
        }
    }

    private String color(JTextField field) {
        String raw = field.getText().trim();
        if (!raw.startsWith("#")) {
            raw = "#" + raw;
        }
        if (raw.length() != 7) throw new IllegalArgumentException("bad color");
        return raw.toUpperCase();
    }

    private String stripHash(String c) {
        return c.startsWith("#") ? c.substring(1) : c;
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public boolean isConvertFullWidthEnabled() {
        return convertFullWidthBox.isSelected();
    }

    public Optional<EditorStyle> getSelectedStyle() {
        return Optional.ofNullable(selectedStyle);
    }
}
