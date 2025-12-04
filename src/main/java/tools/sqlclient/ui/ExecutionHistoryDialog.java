package tools.sqlclient.ui;

import tools.sqlclient.db.SqlHistoryRepository;
import tools.sqlclient.model.SqlHistoryEntry;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;

public class ExecutionHistoryDialog extends JDialog {
    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final SqlHistoryRepository repository;
    private final Consumer<String> insertHandler;
    private final DefaultListModel<SqlHistoryEntry> listModel = new DefaultListModel<>();
    private final JList<SqlHistoryEntry> historyList = new JList<>(listModel);
    private final JButton deleteButton = new JButton("删除选中");
    private final JButton clearButton = new JButton("清空全部");
    private final JButton closeButton = new JButton("关闭");
    private final JLabel feedbackLabel = new JLabel(" ");

    public ExecutionHistoryDialog(Frame owner, SqlHistoryRepository repository, Consumer<String> insertHandler) {
        super(owner, "执行历史", false);
        this.repository = repository;
        this.insertHandler = insertHandler;
        setModal(false);
        setSize(760, 520);
        setLocationRelativeTo(owner);
        setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);
        setLayout(new BorderLayout(8, 8));
        buildUI();
        registerActions();
    }

    public void toggleVisible() {
        refreshList();
        if (isVisible() && isActive()) {
            setVisible(false);
            return;
        }
        setLocationRelativeTo(getOwner());
        setVisible(true);
        toFront();
        SwingUtilities.invokeLater(() -> {
            historyList.requestFocusInWindow();
            if (!listModel.isEmpty()) {
                historyList.setSelectedIndex(0);
                historyList.ensureIndexIsVisible(0);
            }
        });
    }

    private void buildUI() {
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.setBorder(new EmptyBorder(10, 10, 10, 10));

        historyList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        historyList.setCellRenderer(new HistoryRenderer());
        historyList.setFixedCellHeight(72);
        JScrollPane scrollPane = new JScrollPane(historyList);
        content.add(scrollPane, BorderLayout.CENTER);

        JPanel bottom = new JPanel(new BorderLayout());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 6));
        buttons.add(deleteButton);
        buttons.add(clearButton);
        buttons.add(closeButton);
        bottom.add(buttons, BorderLayout.EAST);
        feedbackLabel.setForeground(new Color(0x2d9cdb));
        feedbackLabel.setBorder(new EmptyBorder(6, 6, 6, 6));
        bottom.add(feedbackLabel, BorderLayout.WEST);
        content.add(bottom, BorderLayout.SOUTH);

        setContentPane(content);
    }

    private void registerActions() {
        historyList.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_ENTER) {
                    insertSelection();
                } else if (e.getKeyCode() == KeyEvent.VK_DELETE) {
                    deleteSelection();
                }
            }
        });
        historyList.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                if (e.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(e)) {
                    insertSelection();
                }
            }
        });

        deleteButton.addActionListener(e -> deleteSelection());
        clearButton.addActionListener(this::onClearAll);
        closeButton.addActionListener(e -> setVisible(false));

        getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW)
                .put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "close_dialog");
        getRootPane().getActionMap().put("close_dialog", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setVisible(false);
            }
        });
    }

    private void refreshList() {
        listModel.clear();
        List<SqlHistoryEntry> entries = repository.listAll();
        for (SqlHistoryEntry entry : entries) {
            listModel.addElement(entry);
        }
        feedbackLabel.setText(entries.isEmpty() ? "暂无历史记录" : "共 " + entries.size() + " 条历史记录");
    }

    private void insertSelection() {
        SqlHistoryEntry entry = historyList.getSelectedValue();
        if (entry == null || insertHandler == null) {
            return;
        }
        insertHandler.accept(entry.getSqlText());
        feedbackLabel.setText("已插入到编辑器");
    }

    private void deleteSelection() {
        SqlHistoryEntry entry = historyList.getSelectedValue();
        if (entry == null) {
            JOptionPane.showMessageDialog(this, "请先选择要删除的历史记录", "提示", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "确定删除该条历史记录？", "确认删除", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            repository.delete(entry.getId());
            refreshList();
        }
    }

    private void onClearAll(ActionEvent e) {
        if (listModel.isEmpty()) {
            return;
        }
        int opt = JOptionPane.showConfirmDialog(this,
                "清空全部执行历史？此操作不可恢复。",
                "确认清空", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
        if (opt == JOptionPane.YES_OPTION) {
            repository.clearAll();
            refreshList();
        }
    }

    private static class HistoryRenderer extends DefaultListCellRenderer {
        @Override
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
            Component c = super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            if (c instanceof JLabel label && value instanceof SqlHistoryEntry entry) {
                StringBuilder sb = new StringBuilder();
                String preview = entry.getSqlText().replaceAll("\n", "  ");
                if (preview.length() > 160) {
                    preview = preview.substring(0, 160) + "...";
                }
                sb.append(preview);
                sb.append("  ·  ").append(TIME_FMT.format(Instant.ofEpochMilli(entry.getLastExecutedAt())));
                if (entry.getDatabaseType() != null) {
                    sb.append("  ·  ").append(entry.getDatabaseType().name());
                }
                label.setText("<html>" + sb + "</html>");
                label.setFont(label.getFont().deriveFont(Font.PLAIN, 13f));
                label.setBorder(new EmptyBorder(6, 8, 6, 8));
                if (!isSelected) {
                    label.setBackground(new Color(0xF7F9FB));
                }
            }
            return c;
        }
    }
}
