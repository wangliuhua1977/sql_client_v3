package tools.sqlclient.ui;

import tools.sqlclient.db.SqlSnippetRepository;
import tools.sqlclient.model.SqlSnippet;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.Toolkit;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * SQL 快捷片段窗口：支持搜索、复制、增删改。
 */
public class QuickSqlSnippetDialog extends JDialog {
    private final SqlSnippetRepository snippetRepository;
    private final JTextField searchField = new JTextField();
    private final DefaultListModel<SqlSnippet> listModel = new DefaultListModel<>();
    private final JList<SqlSnippet> snippetList = new JList<>(listModel);
    private final JButton addButton = new JButton("新增片段");
    private final JButton editButton = new JButton("编辑片段");
    private final JButton deleteButton = new JButton("删除片段");
    private final JButton closeButton = new JButton("关闭");
    private final JLabel feedbackLabel = new JLabel(" ");
    private List<SqlSnippet> allSnippets = new ArrayList<>();

    public QuickSqlSnippetDialog(Frame owner, SqlSnippetRepository repository) {
        super(owner, "SQL 快捷片段", false);
        this.snippetRepository = repository;
        setModal(false);
        setSize(640, 500);
        setLocationRelativeTo(owner);
        setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        setLayout(new BorderLayout(8, 8));
        buildUI();
        registerActions();
        refreshList();
    }

    public void toggleVisible() {
        if (isVisible() && isActive()) {
            setVisible(false);
            return;
        }
        refreshList();
        setLocationRelativeTo(getOwner());
        setVisible(true);
        toFront();
        SwingUtilities.invokeLater(() -> {
            searchField.requestFocusInWindow();
            if (!listModel.isEmpty()) {
                snippetList.setSelectedIndex(0);
                snippetList.ensureIndexIsVisible(0);
            }
        });
    }

    private void buildUI() {
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.setBorder(new EmptyBorder(10, 10, 10, 10));

        JPanel searchPanel = new JPanel(new BorderLayout(6, 6));
        searchField.setColumns(28);
        searchField.putClientProperty("JTextField.placeholderText", "输入关键字筛选片段…");
        searchField.setToolTipText("输入关键字筛选片段…");
        searchPanel.add(searchField, BorderLayout.CENTER);
        content.add(searchPanel, BorderLayout.NORTH);

        snippetList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        snippetList.setCellRenderer(new SnippetCellRenderer());
        snippetList.setFixedCellHeight(48);
        JScrollPane scrollPane = new JScrollPane(snippetList);
        content.add(scrollPane, BorderLayout.CENTER);

        JPanel bottom = new JPanel(new BorderLayout());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 6));
        buttons.add(addButton);
        buttons.add(editButton);
        buttons.add(deleteButton);
        buttons.add(closeButton);
        bottom.add(buttons, BorderLayout.EAST);
        feedbackLabel.setForeground(new Color(0x2d9cdb));
        feedbackLabel.setBorder(new EmptyBorder(6, 6, 6, 6));
        bottom.add(feedbackLabel, BorderLayout.WEST);

        content.add(bottom, BorderLayout.SOUTH);
        setContentPane(content);
    }

    private void registerActions() {
        searchField.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            @Override
            public void insertUpdate(javax.swing.event.DocumentEvent e) {
                applyFilter();
            }

            @Override
            public void removeUpdate(javax.swing.event.DocumentEvent e) {
                applyFilter();
            }

            @Override
            public void changedUpdate(javax.swing.event.DocumentEvent e) {
                applyFilter();
            }
        });

        searchField.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_DOWN && !listModel.isEmpty()) {
                    snippetList.requestFocusInWindow();
                    snippetList.setSelectedIndex(0);
                    snippetList.ensureIndexIsVisible(0);
                }
            }
        });

        snippetList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(e)) {
                    copySelectionToClipboard();
                }
                if (SwingUtilities.isRightMouseButton(e)) {
                    showContextMenu(e);
                }
            }

            @Override
            public void mousePressed(MouseEvent e) {
                if (SwingUtilities.isRightMouseButton(e)) {
                    int index = snippetList.locationToIndex(e.getPoint());
                    if (index >= 0) {
                        snippetList.setSelectedIndex(index);
                    }
                    showContextMenu(e);
                }
            }
        });

        snippetList.getInputMap(JComponent.WHEN_FOCUSED)
                .put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), "copy_snippet");
        snippetList.getActionMap().put("copy_snippet", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                copySelectionToClipboard();
            }
        });

        getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW)
                .put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "close_dialog");
        getRootPane().getActionMap().put("close_dialog", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setVisible(false);
            }
        });

        addButton.addActionListener(e -> createSnippet());
        editButton.addActionListener(e -> editSelected());
        deleteButton.addActionListener(e -> deleteSelected());
        closeButton.addActionListener(e -> setVisible(false));
    }

    private void showContextMenu(MouseEvent e) {
        if (e == null || !e.isPopupTrigger() && !SwingUtilities.isRightMouseButton(e)) {
            return;
        }
        JPopupMenu menu = new JPopupMenu();
        JMenuItem copy = new JMenuItem("复制到剪贴板");
        JMenuItem edit = new JMenuItem("编辑片段");
        JMenuItem delete = new JMenuItem("删除片段");
        copy.addActionListener(evt -> copySelectionToClipboard());
        edit.addActionListener(evt -> editSelected());
        delete.addActionListener(evt -> deleteSelected());
        menu.add(copy);
        menu.add(edit);
        menu.add(delete);
        menu.show(snippetList, e.getX(), e.getY());
    }

    private void createSnippet() {
        SnippetEditDialog dialog = new SnippetEditDialog((Frame) getOwner(), "新增片段", null);
        dialog.setVisible(true);
        if (dialog.isConfirmed()) {
            SqlSnippet snippet = snippetRepository.create(dialog.getNameInput(), dialog.getTagsInput(), dialog.getContentInput());
            allSnippets.add(0, snippet);
            applyFilter();
            feedbackLabel.setText("已保存新片段");
        }
    }

    private void editSelected() {
        SqlSnippet selected = snippetList.getSelectedValue();
        if (selected == null) {
            JOptionPane.showMessageDialog(this, "请先选择要编辑的片段", "提示", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        SnippetEditDialog dialog = new SnippetEditDialog((Frame) getOwner(), "编辑片段", selected);
        dialog.setVisible(true);
        if (dialog.isConfirmed()) {
            selected.setName(dialog.getNameInput());
            selected.setTags(dialog.getTagsInput());
            selected.setContent(dialog.getContentInput());
            snippetRepository.update(selected);
            refreshList();
            feedbackLabel.setText("已更新片段");
        }
    }

    private void deleteSelected() {
        SqlSnippet selected = snippetList.getSelectedValue();
        if (selected == null) {
            JOptionPane.showMessageDialog(this, "请先选择要删除的片段", "提示", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        int option = JOptionPane.showConfirmDialog(this,
                "确认要删除选中的 SQL 片段吗？",
                "删除片段",
                JOptionPane.YES_NO_OPTION,
                JOptionPane.WARNING_MESSAGE);
        if (option == JOptionPane.YES_OPTION) {
            snippetRepository.delete(selected.getId());
            refreshList();
            feedbackLabel.setText("已删除片段");
        }
    }

    private void applyFilter() {
        String keyword = searchField.getText() == null ? "" : searchField.getText().trim().toLowerCase(Locale.ROOT);
        listModel.clear();
        for (SqlSnippet snippet : allSnippets) {
            if (keyword.isEmpty()) {
                listModel.addElement(snippet);
                continue;
            }
            String name = snippet.getName() == null ? "" : snippet.getName().toLowerCase(Locale.ROOT);
            String tags = snippet.getTags() == null ? "" : snippet.getTags().toLowerCase(Locale.ROOT);
            String content = snippet.getContent() == null ? "" : snippet.getContent().toLowerCase(Locale.ROOT);
            if (name.contains(keyword) || tags.contains(keyword) || content.contains(keyword)) {
                listModel.addElement(snippet);
            }
        }
        if (!listModel.isEmpty()) {
            snippetList.setSelectedIndex(0);
            snippetList.ensureIndexIsVisible(0);
        }
    }

    private void refreshList() {
        allSnippets = snippetRepository.listAll();
        applyFilter();
    }

    private void copySelectionToClipboard() {
        SqlSnippet selected = snippetList.getSelectedValue();
        if (selected == null) {
            return;
        }
        StringSelection selection = new StringSelection(selected.getContent() == null ? "" : selected.getContent());
        Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, selection);
        feedbackLabel.setText("已复制到剪贴板");
        setVisible(false);
    }

    private static class SnippetCellRenderer extends JPanel implements ListCellRenderer<SqlSnippet> {
        private final JLabel nameLabel = new JLabel();
        private final JLabel previewLabel = new JLabel();

        public SnippetCellRenderer() {
            super(new BorderLayout());
            setBorder(new EmptyBorder(6, 8, 6, 8));
            nameLabel.setFont(nameLabel.getFont().deriveFont(Font.BOLD));
            previewLabel.setFont(previewLabel.getFont().deriveFont(Font.PLAIN, 12f));
            previewLabel.setForeground(Color.GRAY.darker());
            add(nameLabel, BorderLayout.NORTH);
            add(previewLabel, BorderLayout.SOUTH);
        }

        @Override
        public Component getListCellRendererComponent(JList<? extends SqlSnippet> list, SqlSnippet value, int index, boolean isSelected, boolean cellHasFocus) {
            if (value != null) {
                nameLabel.setText(value.getName());
                String text = value.getContent() == null ? "" : value.getContent().replaceAll("\n", " ");
                if (text.length() > 60) {
                    text = text.substring(0, 60) + "...";
                }
                previewLabel.setText(text);
            } else {
                nameLabel.setText("(空)");
                previewLabel.setText("");
            }
            if (isSelected) {
                setBackground(list.getSelectionBackground());
                setForeground(list.getSelectionForeground());
            } else {
                setBackground(list.getBackground());
                setForeground(list.getForeground());
            }
            setOpaque(true);
            return this;
        }
    }

    private static class SnippetEditDialog extends JDialog {
        private final JTextField nameField = new JTextField();
        private final JTextField tagsField = new JTextField();
        private final JTextArea contentArea = new JTextArea();
        private boolean confirmed = false;

        public SnippetEditDialog(Frame owner, String title, SqlSnippet snippet) {
            super(owner, title, true);
            setSize(520, 420);
            setLocationRelativeTo(owner);
            setLayout(new BorderLayout(8, 8));
            JPanel panel = new JPanel(new BorderLayout(6, 6));
            panel.setBorder(new EmptyBorder(10, 10, 10, 10));

            JPanel form = new JPanel();
            form.setLayout(new BoxLayout(form, BoxLayout.Y_AXIS));
            JPanel nameRow = new JPanel(new BorderLayout(6, 6));
            nameRow.add(new JLabel("名称"), BorderLayout.WEST);
            nameRow.add(nameField, BorderLayout.CENTER);
            JPanel tagRow = new JPanel(new BorderLayout(6, 6));
            tagRow.add(new JLabel("标签"), BorderLayout.WEST);
            tagRow.add(tagsField, BorderLayout.CENTER);
            JPanel contentRow = new JPanel(new BorderLayout(6, 6));
            contentRow.add(new JLabel("SQL 内容"), BorderLayout.NORTH);
            contentArea.setLineWrap(true);
            contentArea.setWrapStyleWord(true);
            JScrollPane contentScroll = new JScrollPane(contentArea);
            contentScroll.setPreferredSize(new Dimension(480, 260));
            contentRow.add(contentScroll, BorderLayout.CENTER);
            form.add(nameRow);
            form.add(Box.createVerticalStrut(8));
            form.add(tagRow);
            form.add(Box.createVerticalStrut(8));
            form.add(contentRow);

            panel.add(form, BorderLayout.CENTER);

            JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 6));
            JButton ok = new JButton("确定");
            JButton cancel = new JButton("取消");
            buttons.add(ok);
            buttons.add(cancel);
            panel.add(buttons, BorderLayout.SOUTH);
            setContentPane(panel);

            if (snippet != null) {
                nameField.setText(snippet.getName());
                tagsField.setText(snippet.getTags());
                contentArea.setText(snippet.getContent());
            }

            ok.addActionListener(e -> onConfirm());
            cancel.addActionListener(e -> setVisible(false));
        }

        private void onConfirm() {
            String name = nameField.getText() == null ? "" : nameField.getText().trim();
            if (name.isEmpty()) {
                JOptionPane.showMessageDialog(this, "名称不能为空", "提示", JOptionPane.WARNING_MESSAGE);
                return;
            }
            confirmed = true;
            setVisible(false);
        }

        public boolean isConfirmed() {
            return confirmed;
        }

        public String getNameInput() {
            return nameField.getText() == null ? "" : nameField.getText().trim();
        }

        public String getTagsInput() {
            return tagsField.getText() == null ? "" : tagsField.getText().trim();
        }

        public String getContentInput() {
            return contentArea.getText() == null ? "" : contentArea.getText();
        }
    }
}
