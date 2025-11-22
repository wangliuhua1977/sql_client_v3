package tools.sqlclient.util;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.metadata.MetadataService.SuggestionContext;
import tools.sqlclient.metadata.MetadataService.SuggestionItem;
import tools.sqlclient.metadata.MetadataService.SuggestionType;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 SQLite 元数据的模糊联想。
 */
public class SuggestionEngine {
    private final MetadataService metadataService;
    private final RSyntaxTextArea textArea;
    private final JPopupMenu popup = new JPopupMenu();
    private final JList<SuggestionItem> list = new JList<>();
    private List<SuggestionItem> currentItems = List.of();
    private int replaceStart = -1;
    private int replaceEnd = -1;

    public SuggestionEngine(MetadataService metadataService, RSyntaxTextArea textArea) {
        this.metadataService = metadataService;
        this.textArea = textArea;
        popup.setFocusable(false);
        popup.add(new JScrollPane(list));
        list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        list.setCellRenderer(new DefaultListCellRenderer() {
            @Override
            public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
                JLabel label = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
                if (value instanceof SuggestionItem item) {
                    String suffix = Objects.equals(item.type(), "column") && item.tableHint() != null ?
                            "  (" + item.tableHint() + ")" : "";
                    label.setText(item.name() + suffix);
                    if (item.useCount() > 0) {
                        label.setFont(label.getFont().deriveFont(Font.BOLD));
                    }
                }
                return label;
            }
        });
        list.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                if (e.getClickCount() == 2) {
                    commitSelection();
                }
            }
        });
    }

    public KeyAdapter createKeyListener(AtomicBoolean ctrlSpaceEnabled) {
        return new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (popup.isVisible()) {
                    if (e.getKeyCode() == KeyEvent.VK_DOWN) {
                        int next = Math.min(list.getSelectedIndex() + 1, list.getModel().getSize() - 1);
                        list.setSelectedIndex(next);
                        list.ensureIndexIsVisible(next);
                        e.consume();
                        return;
                    } else if (e.getKeyCode() == KeyEvent.VK_UP) {
                        int prev = Math.max(list.getSelectedIndex() - 1, 0);
                        list.setSelectedIndex(prev);
                        list.ensureIndexIsVisible(prev);
                        e.consume();
                        return;
                    } else if (e.getKeyCode() == KeyEvent.VK_ENTER) {
                        commitSelection();
                        e.consume();
                        return;
                    }
                }
                if (e.getKeyCode() == KeyEvent.VK_SPACE && e.isControlDown()) {
                    ctrlSpaceEnabled.set(!ctrlSpaceEnabled.get());
                    JOptionPane.showMessageDialog(textArea, "自动联想已" + (ctrlSpaceEnabled.get() ? "开启" : "关闭"));
                }
            }

            @Override
            public void keyReleased(KeyEvent e) {
                if (!ctrlSpaceEnabled.get()) {
                    popup.setVisible(false);
                    return;
                }
                if (popup.isVisible() && (e.getKeyCode() == KeyEvent.VK_UP || e.getKeyCode() == KeyEvent.VK_DOWN)) {
                    e.consume();
                    return;
                }
                SuggestionContext ctx = analyzeContext();
                if (ctx != null && (shouldTrigger(e) || popup.isVisible())) {
                    String prefix = currentToken();
                    if (prefix.contains(".")) {
                        prefix = prefix.substring(prefix.lastIndexOf('.') + 1);
                    }
                    showSuggestions(prefix, ctx);
                } else {
                    popup.setVisible(false);
                }
            }
        };
    }

    private boolean shouldTrigger(KeyEvent e) {
        char ch = e.getKeyChar();
        int code = e.getKeyCode();
        return Character.isAlphabetic(ch) || Character.isDigit(ch) || ch == '%' || ch == '.' || ch == '_' ||
                code == KeyEvent.VK_PERIOD || code == KeyEvent.VK_DECIMAL || code == KeyEvent.VK_SPACE;
    }

    private String currentToken() {
        int caret = textArea.getCaretPosition();
        try {
            String before = textArea.getText(0, caret);
            int start = Math.max(Math.max(before.lastIndexOf(' '), before.lastIndexOf('\n')), before.lastIndexOf('\t')) + 1;
            int length = caret - start;
            return textArea.getText(start, length).trim();
        } catch (Exception e) {
            return "";
        }
    }

    private void showSuggestions(String token, SuggestionContext context) {
        showSuggestions(token, context, false);
    }

    private void showSuggestions(String token, SuggestionContext context, boolean skipFetch) {
        boolean refreshing = false;
        if (!skipFetch && context.type() == SuggestionType.COLUMN && context.tableHint() != null) {
            refreshing = metadataService.ensureColumnsCachedAsync(context.tableHint(),
                    () -> SwingUtilities.invokeLater(() -> showSuggestions(token, context, true)));
        }
        currentItems = metadataService.suggest(token, context, 15);
        computeReplacementRange(token);
        if (currentItems.isEmpty()) {
            if (refreshing) {
                list.setListData(new SuggestionItem[]{new SuggestionItem("加载中...", "loading", null, 0)});
                list.setSelectedIndex(0);
                showPopup();
            } else {
                popup.setVisible(false);
            }
            return;
        }
        int keepIndex = list.getSelectedIndex();
        list.setListData(currentItems.toArray(new SuggestionItem[0]));
        if (keepIndex >= 0 && keepIndex < list.getModel().getSize()) {
            list.setSelectedIndex(keepIndex);
        } else {
            list.setSelectedIndex(0);
        }
        showPopup();
    }

    private void showPopup() {
        try {
            Rectangle view = textArea.modelToView(textArea.getCaretPosition());
            popup.show(textArea, view.x, view.y + view.height);
        } catch (Exception ignored) {
            popup.setVisible(false);
        }
    }

    private void commitSelection() {
        SuggestionItem item = list.getSelectedValue();
        if (item == null || "loading".equals(item.type())) return;
        insertText(item.name());
        if ("table".equals(item.type()) || "view".equals(item.type())) {
            metadataService.ensureColumnsCachedAsync(item.name());
        }
        metadataService.recordUsage(item);
        popup.setVisible(false);
    }

    private void insertText(String text) {
        try {
            int start = replaceStart >= 0 ? replaceStart : textArea.getSelectionStart();
            int end = replaceEnd >= 0 ? replaceEnd : textArea.getSelectionEnd();
            textArea.getDocument().remove(start, end - start);
            textArea.getDocument().insertString(start, text, null);
        } catch (Exception ignored) {
        }
    }

    private void computeReplacementRange(String token) {
        try {
            int caret = textArea.getCaretPosition();
            if (token.contains(".")) {
                int dot = token.lastIndexOf('.') + 1;
                int len = token.length() - dot;
                replaceStart = caret - len;
                replaceEnd = caret;
            } else {
                replaceStart = caret - token.length();
                replaceEnd = caret;
            }
        } catch (Exception e) {
            replaceStart = replaceEnd = -1;
        }
    }

    private SuggestionContext analyzeContext() {
        int caret = textArea.getCaretPosition();
        String content = textArea.getText();
        if (caret <= 0 || content == null) return null;
        String before = content.substring(0, caret);
        String line = before.substring(before.lastIndexOf('\n') + 1);
        if (line.trim().startsWith("--")) {
            return null; // 注释行不联想
        }
        String token = currentToken();
        if (token.contains(".")) {
            String[] parts = token.split("\\.");
            if (parts.length >= 1) {
                String aliasOrTable = parts[0];
                String table = resolveAlias(aliasOrTable, before);
                if (table == null && metadataService.isKnownTableOrViewCached(aliasOrTable)) {
                    table = aliasOrTable; // 直接输入表名的场景
                }
                if (table != null) {
                    return new SuggestionContext(SuggestionType.COLUMN, table);
                }
            }
        }

        String lower = before.toLowerCase();
        if (endsWithKeyword(lower, List.of(" from ", " join ", " into ", " update ", " delete from ", " insert into ", " truncate ", " table ", " view " ))) {
            return new SuggestionContext(SuggestionType.TABLE_OR_VIEW, null);
        }
        if (endsWithKeyword(lower, List.of(" call ", " exec ", " execute " ))) {
            return new SuggestionContext(SuggestionType.FUNCTION_OR_PROCEDURE, null);
        }
        return null;
    }

    private boolean endsWithKeyword(String text, List<String> keywords) {
        for (String kw : keywords) {
            int idx = text.lastIndexOf(kw);
            if (idx >= 0 && idx >= text.length() - kw.length() - currentToken().length() - 1) {
                return true;
            }
        }
        return false;
    }

    private String resolveAlias(String alias, String before) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "(?i)(from|join)\\s+([\\w.]+)(?:\\s+(?:as\\s+)?([\\w]+))?");
        java.util.regex.Matcher matcher = pattern.matcher(before);
        String resolved = null;
        while (matcher.find()) {
            String tableToken = matcher.group(2);
            String aliasToken = matcher.group(3);
            if (aliasToken != null && aliasToken.equals(alias)) {
                resolved = tableToken.contains(".") ? tableToken.substring(tableToken.lastIndexOf('.') + 1) : tableToken;
            } else if (aliasToken == null) {
                String base = tableToken.contains(".") ? tableToken.substring(tableToken.lastIndexOf('.') + 1) : tableToken;
                if (base.equals(alias)) {
                    resolved = base;
                }
            }
        }
        return resolved;
    }
}
