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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 SQLite 元数据的模糊联想。
 */
public class SuggestionEngine {
    private final MetadataService metadataService;
    private final RSyntaxTextArea textArea;
    private final JPopupMenu popup = new JPopupMenu();
    private final JList<SuggestionItem> list = new JList<>();
    private static final List<KeywordRule> KEYWORD_RULES = List.of(
            new KeywordRule(List.of("delete from", "insert into", "from", "join", "where", "into", "update", "truncate", "table", "view"), SuggestionType.TABLE_OR_VIEW),
            new KeywordRule(List.of("select"), SuggestionType.FUNCTION),
            new KeywordRule(List.of("call", "exec", "execute"), SuggestionType.PROCEDURE)
    );
    private List<SuggestionItem> currentItems = List.of();
    private int replaceStart = -1;
    private int replaceEnd = -1;
    private boolean showTableHintForColumns = true;
    private SuggestionContext lastContext;
    private volatile boolean objectsRefreshInFlight = false;
    private long lastObjectsRefreshAt = 0L;

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
                    String suffix = (Objects.equals(item.type(), "column") && item.tableHint() != null && showTableHintForColumns)
                            ? "  (" + item.tableHint() + ")" : "";
                    String count = " [" + item.useCount() + "]";
                    label.setText(item.name() + suffix + ("all_columns".equals(item.type()) ? "" : count));
                    if (item.useCount() > 0 || "all_columns".equals(item.type())) {
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

    public KeyAdapter createKeyListener() {
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
            }

            @Override
            public void keyReleased(KeyEvent e) {
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
            String text = textArea.getText(0, caret);
            int start = caret - 1;
            while (start >= 0) {
                char ch = text.charAt(start);
                if (Character.isLetterOrDigit(ch) || ch == '_' || ch == '%' || ch == '.') {
                    start--;
                } else {
                    break;
                }
            }
            start = start + 1;
            return text.substring(start).trim();
        } catch (Exception e) {
            return "";
        }
    }

    private void showSuggestions(String token, SuggestionContext context) {
        showSuggestions(token, context, false);
    }

    private void showSuggestions(String token, SuggestionContext context, boolean skipFetch) {
        boolean refreshing = false;
        this.showTableHintForColumns = context.showTableHint();
        this.lastContext = context;
        if (!skipFetch && context.type() == SuggestionType.COLUMN && context.tableHint() != null) {
            refreshing = metadataService.ensureColumnsCachedAsync(context.tableHint(),
                    () -> SwingUtilities.invokeLater(() -> showSuggestions(token, context, true)));
        }
        currentItems = metadataService.suggest(token, context, 15);
        if (currentItems.isEmpty() && !skipFetch && context.type() == SuggestionType.TABLE_OR_VIEW) {
            triggerObjectRefresh(token, context);
            return;
        }
        if (context.type() == SuggestionType.COLUMN && context.tableHint() != null && !currentItems.isEmpty()) {
            List<SuggestionItem> withAll = new ArrayList<>();
            withAll.add(new SuggestionItem("所有字段", "all_columns", context.tableHint(), 0));
            withAll.addAll(currentItems);
            currentItems = withAll;
        }
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
        if ("all_columns".equals(item.type())) {
            if (lastContext == null || lastContext.tableHint() == null) {
                popup.setVisible(false);
                return;
            }
            java.util.List<String> cols = metadataService.loadColumnsFromCache(lastContext.tableHint());
            if (cols.isEmpty()) {
                JOptionPane.showMessageDialog(textArea, "字段仍在加载，请稍后重试");
                return;
            }
            String prefix = lastContext.alias() != null && !lastContext.alias().isBlank()
                    ? lastContext.alias().trim() + "." : "";
            String joined = String.join(", ", cols.stream().map(c -> prefix + c).toList());
            insertText(joined);
            popup.setVisible(false);
            return;
        }
        String text = item.name();
        if ("function".equalsIgnoreCase(item.type()) || "procedure".equalsIgnoreCase(item.type())) {
            text = item.name() + "()";
        }
        insertText(text);
        if ("table".equals(item.type()) || "view".equals(item.type())) {
            metadataService.ensureColumnsCachedAsync(item.name());
        }
        metadataService.recordUsage(item);
        popup.setVisible(false);
    }

    private void triggerObjectRefresh(String token, SuggestionContext context) {
        long now = System.currentTimeMillis();
        if (objectsRefreshInFlight || now - lastObjectsRefreshAt < 2000L) {
            list.setListData(new SuggestionItem[]{new SuggestionItem("加载中...", "loading", null, 0)});
            list.setSelectedIndex(0);
            showPopup();
            return;
        }
        objectsRefreshInFlight = true;
        lastObjectsRefreshAt = now;
        list.setListData(new SuggestionItem[]{new SuggestionItem("加载中...", "loading", null, 0)});
        list.setSelectedIndex(0);
        showPopup();
        metadataService.refreshMetadataAsync(() -> SwingUtilities.invokeLater(() -> {
            objectsRefreshInFlight = false;
            showSuggestions(token, context, true);
        }));
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
        String statement = currentStatement(content, caret);
        String statementPrefix = currentStatementPrefix(content, caret);
        String line = statementPrefix.substring(statementPrefix.lastIndexOf('\n') + 1);
        if (line.trim().startsWith("--")) {
            return null; // 注释行不联想
        }
        List<TableBinding> bindings = parseBindings(statement);
        int tableCount = (int) bindings.stream().map(TableBinding::table).distinct().count();
        String token = currentToken();
        int tokenStart = caret - token.length();
        if (token.contains(".")) {
            String[] parts = token.split("\\.");
            if (parts.length >= 1) {
                String aliasOrTable = parts[0];
                String table = resolveAlias(aliasOrTable, bindings);
                boolean showHint = false;
                if (table == null && metadataService.isKnownTableOrViewCached(aliasOrTable)) {
                    table = aliasOrTable; // 直接输入表名
                }
                if (table == null && tableCount == 1 && !bindings.isEmpty()) {
                    table = bindings.get(0).table();
                }
                if (table == null) {
                    showHint = tableCount > 1;
                    return new SuggestionContext(SuggestionType.COLUMN, null, showHint, aliasOrTable);
                }
                return new SuggestionContext(SuggestionType.COLUMN, table, false, aliasOrTable);
            }
        }

        KeywordMatch keyword = findLastKeyword(statementPrefix, tokenStart);
        if (keyword != null) {
            if (keyword.type() == SuggestionType.TABLE_OR_VIEW) {
                return new SuggestionContext(SuggestionType.TABLE_OR_VIEW, null, false, null);
            }
            if (keyword.type() == SuggestionType.FUNCTION) {
                return new SuggestionContext(SuggestionType.FUNCTION, null, false, null);
            }
            if (keyword.type() == SuggestionType.PROCEDURE) {
                return new SuggestionContext(SuggestionType.PROCEDURE, null, false, null);
            }
        }
        return null;
    }

    private KeywordMatch findLastKeyword(String prefix, int tokenStart) {
        String lower = prefix.toLowerCase();
        KeywordMatch best = null;
        for (KeywordRule rule : KEYWORD_RULES) {
            for (String kw : rule.keywords()) {
                int searchPos = Math.max(0, tokenStart);
                while (true) {
                    int idx = lower.lastIndexOf(kw, searchPos);
                    if (idx < 0) break;
                    int end = idx + kw.length();
                    if (end > tokenStart) {
                        searchPos = idx - 1;
                        continue;
                    }
                    boolean beforeOk = idx == 0 || !Character.isLetterOrDigit(lower.charAt(idx - 1));
                    boolean afterOk = end >= lower.length() || !Character.isLetterOrDigit(lower.charAt(end));
                    if (beforeOk && afterOk) {
                        if (best == null || end > best.end()) {
                            best = new KeywordMatch(rule.type(), idx, end);
                        }
                        break;
                    }
                    searchPos = idx - 1;
                }
            }
        }
        return best;
    }

    private String currentStatement(String content, int caret) {
        int start = content.lastIndexOf(';', Math.max(0, caret - 1));
        start = (start < 0) ? 0 : start + 1;
        int end = content.indexOf(';', caret);
        end = (end < 0) ? content.length() : end;
        return content.substring(start, end);
    }

    private String currentStatementPrefix(String content, int caret) {
        int start = content.lastIndexOf(';', Math.max(0, caret - 1));
        start = (start < 0) ? 0 : start + 1;
        return content.substring(start, caret);
    }

    private Map<String, String> resolveAliasMap(List<TableBinding> bindings) {
        Map<String, String> map = new HashMap<>();
        for (TableBinding b : bindings) {
            map.putIfAbsent(b.table().toLowerCase(), b.table());
            if (b.alias() != null) {
                map.put(b.alias().toLowerCase(), b.table());
            }
        }
        return map;
    }

    private String resolveAlias(String alias, List<TableBinding> bindings) {
        Map<String, String> map = resolveAliasMap(bindings);
        return map.get(alias.toLowerCase());
    }

    private List<TableBinding> parseBindings(String statement) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?i)(from|join)\\s+([\\w.]+)(?:\\s+(?:as\\s+)?([\\w]+))?");
        java.util.regex.Matcher matcher = pattern.matcher(statement);
        java.util.List<TableBinding> bindings = new java.util.ArrayList<>();
        while (matcher.find()) {
            String tableToken = matcher.group(2);
            String aliasToken = matcher.group(3);
            String base = tableToken.contains(".") ? tableToken.substring(tableToken.lastIndexOf('.') + 1) : tableToken;
            bindings.add(new TableBinding(base, aliasToken));
        }
        return bindings;
    }

    private record TableBinding(String table, String alias) {}
    private record KeywordRule(List<String> keywords, SuggestionType type) {}
    private record KeywordMatch(SuggestionType type, int start, int end) {}
}
