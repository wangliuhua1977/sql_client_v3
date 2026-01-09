package tools.sqlclient.util;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.metadata.MetadataService.SuggestionContext;
import tools.sqlclient.metadata.MetadataService.SuggestionItem;
import tools.sqlclient.metadata.MetadataService.SuggestionType;

import javax.swing.*;
import javax.swing.border.LineBorder;
import javax.swing.event.CaretEvent;
import javax.swing.event.CaretListener;
import java.awt.*;
import java.awt.event.AWTEventListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.Rectangle2D;
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
    private final ResizableSuggestionDialog popup;
    private final JScrollPane scrollPane;
    private final JList<SuggestionItem> list = new JList<>();
    private static final List<String> TABLE_KEYWORDS = List.of("update", "delete", "truncate", "drop table", "exists", "from", "join", "where");
    private static final List<String> COLUMN_KEYWORDS = List.of("on");
    private static final List<String> FUNCTION_KEYWORDS = List.of("select");
    private static final List<String> PROCEDURE_KEYWORDS = List.of("call");
    private List<SuggestionItem> currentItems = List.of();
    private int replaceStart = -1;
    private int replaceEnd = -1;
    private boolean showTableHintForColumns = true;
    private SuggestionContext lastContext;
    private volatile boolean enabled = true;
    private volatile boolean suppressCaretDismiss;
    private volatile long suppressCaretDismissUntil;
    private int lastCaretPosition = -1;
    private AWTEventListener globalMouseListener;
    private static final long CARET_SUPPRESS_WINDOW_MS = 80;
    private static final boolean POPUP_DEBUG = "true".equalsIgnoreCase(Config.getRawProperty("suggestion.popup.debug"));
    private String lastHideReason;
    private Boolean popupVisibilityOverride;

    public SuggestionEngine(MetadataService metadataService, RSyntaxTextArea textArea) {
        this.metadataService = metadataService;
        this.textArea = textArea;
        this.scrollPane = new JScrollPane(list);
        this.popup = new ResizableSuggestionDialog(SwingUtilities.getWindowAncestor(textArea));
        popup.getContentPane().add(scrollPane, BorderLayout.CENTER);
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
        installDismissListeners();
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (!enabled) {
            hidePopup("disabled");
        }
    }

    public void closePopup() {
        hidePopup("closePopup");
    }

    public void hidePopup(String reason) {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(() -> hidePopup(reason));
            return;
        }
        boolean wasVisible = isPopupVisible();
        if (!wasVisible && globalMouseListener == null) {
            return;
        }
        if (wasVisible) {
            setPopupVisible(false);
            lastHideReason = reason;
            if (POPUP_DEBUG && OperationLog.isReady()) {
                OperationLog.log("联想窗口已关闭: " + reason);
            }
        }
        unregisterGlobalMouseListener();
    }

    public KeyAdapter createKeyListener() {
        return new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (!enabled) {
                    hidePopup("disabled");
                    return;
                }
                if (isPopupVisible()) {
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
                    } else if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
                        hidePopup("escape");
                        e.consume();
                        return;
                    }
                }
            }

            @Override
            public void keyReleased(KeyEvent e) {
                if (!enabled) {
                    hidePopup("disabled");
                    return;
                }

                // 弹窗打开时，用上下键只移动选中项，不触发重新检索
                if (isPopupVisible()
                        && (e.getKeyCode() == KeyEvent.VK_UP || e.getKeyCode() == KeyEvent.VK_DOWN)) {
                    e.consume();
                    return;
                }

                // 分号：结束语句并关闭联想
                if (e.getKeyChar() == ';') {
                    hidePopup("statementEnd");
                    return;
                }

                boolean activationKey = isActivationKey(e); // 空格 / 点
                boolean filterKey = isFilterKey(e);         // 字母、数字、退格等

                if (!activationKey && !filterKey) {
                    // 其它键（Ctrl 组合、Home、End 等）不动联想状态
                    return;
                }

                // 根据当前光标重新分析上下文
                SuggestionContext ctx = analyzeContext();

                // 如果这次没分析出来，但弹窗已开且是在输入过滤字符，就沿用上一次上下文
                if (ctx == null && isPopupVisible() && filterKey && lastContext != null) {
                    ctx = lastContext;
                }

                // 仍然没有上下文，说明光标不在 from/join 后等位置，直接关掉弹窗
                if (ctx == null) {
                    hidePopup("contextMissing");
                    return;
                }

                // 关键：只要有上下文，激活键或过滤键都触发联想，不再依赖“弹窗是否已打开”
                String prefix = currentToken();
                if (prefix.contains(".")) {
                    prefix = prefix.substring(prefix.lastIndexOf('.') + 1);
                }
                showSuggestions(prefix, ctx);
            }
        };
    }




    private boolean isActivationKey(KeyEvent e) {
        int code = e.getKeyCode();
        char ch = e.getKeyChar();
        return code == KeyEvent.VK_SPACE || code == KeyEvent.VK_PERIOD || code == KeyEvent.VK_DECIMAL || ch == '.';
    }

    private boolean isFilterKey(KeyEvent e) {
        int code = e.getKeyCode();

        // 退格 / 删除：也视为需要刷新联想
        if (code == KeyEvent.VK_BACK_SPACE || code == KeyEvent.VK_DELETE) {
            return true;
        }

        // 带 Ctrl / Alt / Meta 的快捷键不算输入字符
        if (e.isControlDown() || e.isAltDown() || e.isMetaDown()) {
            return false;
        }

        char ch = e.getKeyChar();
        // KEY_TYPED 时优先用字符本身
        if (Character.isLetterOrDigit(ch) || ch == '%' || ch == '_' || ch == '.') {
            return true;
        }

        // KEY_RELEASED 下很多键 getKeyChar() 是 CHAR_UNDEFINED，用 keyCode 兜底
        if (code >= KeyEvent.VK_A && code <= KeyEvent.VK_Z) {
            return true;
        }
        if (code >= KeyEvent.VK_0 && code <= KeyEvent.VK_9) {
            return true;
        }

        return false;
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
            return text.substring(start, caret).trim();
        } catch (Exception e) {
            return "";
        }
    }

    private void showSuggestions(String token, SuggestionContext context) {
        showSuggestions(token, context, false);
    }

    private void showSuggestions(String token, SuggestionContext context, boolean skipFetch) {
        if (!enabled) {
            hidePopup("disabled");
            return;
        }
        boolean refreshing = false;
        this.showTableHintForColumns = context.showTableHint();
        this.lastContext = context;
        if (!skipFetch && context.type() == SuggestionType.COLUMN && context.tableHint() != null) {
            refreshing = metadataService.ensureColumnsCachedAsync(context.tableHint(),
                    () -> SwingUtilities.invokeLater(() -> showSuggestions(token, context, true)));
        }
        currentItems = metadataService.suggest(token, context, 15);
        if (currentItems.isEmpty()) {
            if (context.type() == SuggestionType.TABLE_OR_VIEW) {
                list.setListData(new SuggestionItem[]{new SuggestionItem("无本地匹配，使用工具菜单刷新元数据", "hint", null, 0)});
                list.setSelectedIndex(0);
                showPopup();
            } else {
                hidePopup("emptyItems");
            }
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
                hidePopup("emptyItems");
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
            if (popupVisibilityOverride != null) {
                popupVisibilityOverride = true;
                return;
            }
            Rectangle2D view = textArea.modelToView2D(textArea.getCaretPosition());
            if (view != null) {
                popup.showAt(textArea, view.getBounds());
            }
            if (isPopupVisible()) {
                registerGlobalMouseListener();
            }
        } catch (Exception ignored) {
            hidePopup("showFailed");
        }
    }

    private void commitSelection() {
        SuggestionItem item = list.getSelectedValue();
        if (item == null || "loading".equals(item.type()) || "hint".equals(item.type())) return;
        if ("all_columns".equals(item.type())) {
            if (lastContext == null || lastContext.tableHint() == null) {
                hidePopup("missingTableHint");
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
            hidePopup("commitAllColumns");
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
        hidePopup("commitItem");
    }

    private void insertText(String text) {
        beginCaretDismissSuppression();
        try {
            int start = replaceStart >= 0 ? replaceStart : textArea.getSelectionStart();
            int end = replaceEnd >= 0 ? replaceEnd : textArea.getSelectionEnd();
            textArea.getDocument().remove(start, end - start);
            textArea.getDocument().insertString(start, text, null);
        } catch (Exception ignored) {
        } finally {
            SwingUtilities.invokeLater(this::endCaretDismissSuppression);
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
        List<String> distinctTables = bindings.stream().map(TableBinding::table).distinct().toList();
        int tableCount = distinctTables.size();

        String token = currentToken();
        // ★ 关键修正：token 起始位置要按「当前语句前缀」来算，而不是整篇文档
        int tokenStartInPrefix = statementPrefix.length() - token.length();
        if (tokenStartInPrefix < 0) {
            tokenStartInPrefix = 0;
        }

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
                    return new SuggestionContext(SuggestionType.COLUMN, null, showHint, aliasOrTable, distinctTables);
                }
                return new SuggestionContext(SuggestionType.COLUMN, table, false, aliasOrTable, distinctTables);
            }
        }

        KeywordMatch onMatch = findKeywordWithWhitespace(statementPrefix, tokenStartInPrefix, COLUMN_KEYWORDS);
        if (onMatch != null) {
            return new SuggestionContext(SuggestionType.COLUMN, null, tableCount > 1, null, distinctTables);
        }

        KeywordMatch tableMatch = findKeywordWithWhitespace(statementPrefix, tokenStartInPrefix, TABLE_KEYWORDS);
        if (tableMatch != null) {
            if ("where".equalsIgnoreCase(tableMatch.keyword()) && !bindings.isEmpty()) {
                if (tableCount == 1) {
                    return new SuggestionContext(SuggestionType.COLUMN, bindings.get(0).table(), false, null, distinctTables);
                }
                return new SuggestionContext(SuggestionType.COLUMN, null, true, null, distinctTables);
            }
            return new SuggestionContext(SuggestionType.TABLE_OR_VIEW, null, false, null, List.of());
        }

        KeywordMatch fnMatch = findKeywordWithWhitespace(statementPrefix, tokenStartInPrefix, FUNCTION_KEYWORDS);
        if (fnMatch != null) {
            return new SuggestionContext(SuggestionType.FUNCTION, null, false, null, List.of());
        }

        KeywordMatch procMatch = findKeywordWithWhitespace(statementPrefix, tokenStartInPrefix, PROCEDURE_KEYWORDS);
        if (procMatch != null) {
            return new SuggestionContext(SuggestionType.PROCEDURE, null, false, null, List.of());
        }
        return null;
    }


    private KeywordMatch findKeywordWithWhitespace(String prefix, int tokenStart, List<String> keywords) {
        String lower = prefix.toLowerCase();
        KeywordMatch best = null;
        for (String kw : keywords) {
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
                if (beforeOk && afterOk && onlyWhitespaceBetween(prefix, end, tokenStart)) {
                    if (best == null || end > best.end()) {
                        best = new KeywordMatch(keywordTypeFor(kw), idx, end, kw);
                    }
                    break;
                }
                searchPos = idx - 1;
            }
        }
        return best;
    }

    private SuggestionType keywordTypeFor(String keyword) {
        if (TABLE_KEYWORDS.contains(keyword)) return SuggestionType.TABLE_OR_VIEW;
        if (COLUMN_KEYWORDS.contains(keyword)) return SuggestionType.COLUMN;
        if (FUNCTION_KEYWORDS.contains(keyword)) return SuggestionType.FUNCTION;
        return SuggestionType.PROCEDURE;
    }

    private boolean onlyWhitespaceBetween(String text, int start, int end) {
        if (text == null || text.isEmpty()) {
            return true;
        }

        // 将输入索引限制在字符串边界内，若区间为空直接返回
        int len = text.length();
        int safeStart = Math.max(0, Math.min(start, len));
        int safeEnd = Math.max(0, Math.min(end, len));
        if (safeStart >= safeEnd) {
            return true;
        }

        // 固定长度后遍历，完全避免 charAt 越界
        for (int i = safeStart; i < safeEnd; i++) {
            if (!Character.isWhitespace(text.charAt(i))) {
                return false;
            }
        }
        return true;
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
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "(?i)(from|join)\\s+([\\w.]+)(?:\\s+(?:as\\s+)?(?!join\\b|where\\b|on\\b|inner\\b|left\\b|right\\b|full\\b|cross\\b|group\\b|order\\b|having\\b|limit\\b|union\\b)([\\w]+))?");
        java.util.regex.Matcher matcher = pattern.matcher(statement);
        java.util.Map<String, TableBinding> map = new java.util.LinkedHashMap<>();
        while (matcher.find()) {
            String tableToken = matcher.group(2);
            String aliasToken = sanitizeAlias(matcher.group(3));
            String base = tableToken.contains(".") ? tableToken.substring(tableToken.lastIndexOf('.') + 1) : tableToken;
            map.putIfAbsent(base.toLowerCase(), new TableBinding(base, aliasToken));
            if (aliasToken != null && !aliasToken.isBlank()) {
                map.putIfAbsent(aliasToken.toLowerCase(), new TableBinding(base, aliasToken));
            }
        }

        java.util.regex.Matcher fromMatcher = java.util.regex.Pattern
                .compile("(?i)from\\s+(.+?)(?=\\bwhere\\b|\\bgroup\\b|\\border\\b|\\bhaving\\b|\\blimit\\b|$)")
                .matcher(statement);
        if (fromMatcher.find()) {
            String fromSegment = fromMatcher.group(1);
            String[] parts = fromSegment.split(",");
            java.util.regex.Pattern item = java.util.regex.Pattern.compile("(?i)([\\w.]+)(?:\\s+(?:as\\s+)?([\\w]+))?");
            for (String raw : parts) {
                java.util.regex.Matcher m = item.matcher(raw.trim());
                if (m.find()) {
                    String table = m.group(1);
                    String alias = sanitizeAlias(m.group(2));
                    String base = table.contains(".") ? table.substring(table.lastIndexOf('.') + 1) : table;
                    map.putIfAbsent(base.toLowerCase(), new TableBinding(base, alias));
                    if (alias != null && !alias.isBlank()) {
                        map.putIfAbsent(alias.toLowerCase(), new TableBinding(base, alias));
                    }
                }
            }
        }
        return new java.util.ArrayList<>(new java.util.LinkedHashSet<>(map.values()));
    }

    private String sanitizeAlias(String aliasToken) {
        if (aliasToken == null) return null;
        String alias = aliasToken.trim();
        if (alias.isEmpty()) return null;
        String lower = alias.toLowerCase();
        java.util.Set<String> reserved = java.util.Set.of(
                "join", "on", "where", "inner", "left", "right", "full", "cross", "group", "order", "having", "limit", "union"
        );
        return reserved.contains(lower) ? null : alias;
    }

    private record TableBinding(String table, String alias) {}
    private record KeywordMatch(SuggestionType type, int start, int end, String keyword) {}

    private void installDismissListeners() {
        textArea.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                hidePopup("focusLost");
            }
        });
        CaretListener caretListener = new CaretListener() {
            @Override
            public void caretUpdate(CaretEvent e) {
                int dot = e.getDot();
                if (lastCaretPosition == -1) {
                    lastCaretPosition = dot;
                    return;
                }
                boolean changed = dot != lastCaretPosition;
                lastCaretPosition = dot;
                if (!changed || !isPopupVisible()) {
                    return;
                }
                if (isCaretDismissSuppressed()) {
                    return;
                }
                hidePopup("caretMoved");
            }
        };
        textArea.addCaretListener(caretListener);
        lastCaretPosition = textArea.getCaretPosition();
    }

    private void beginCaretDismissSuppression() {
        suppressCaretDismiss = true;
        suppressCaretDismissUntil = System.currentTimeMillis() + CARET_SUPPRESS_WINDOW_MS;
    }

    private void endCaretDismissSuppression() {
        suppressCaretDismiss = false;
        suppressCaretDismissUntil = 0L;
    }

    private boolean isCaretDismissSuppressed() {
        if (!suppressCaretDismiss) {
            return false;
        }
        if (System.currentTimeMillis() > suppressCaretDismissUntil) {
            suppressCaretDismiss = false;
            suppressCaretDismissUntil = 0L;
            return false;
        }
        return true;
    }

    private void registerGlobalMouseListener() {
        if (globalMouseListener != null) {
            return;
        }
        globalMouseListener = event -> {
            if (!(event instanceof MouseEvent mouseEvent)) {
                return;
            }
            if (mouseEvent.getID() != MouseEvent.MOUSE_PRESSED) {
                return;
            }
            if (!isPopupVisible()) {
                return;
            }
            Component component = mouseEvent.getComponent();
            if (component == null) {
                hidePopup("globalMouse");
                return;
            }
            if (isComponentInsidePopup(component) || isComponentInsideEditor(component)) {
                return;
            }
            hidePopup("globalMouse");
        };
        Toolkit.getDefaultToolkit().addAWTEventListener(globalMouseListener, AWTEvent.MOUSE_EVENT_MASK);
    }

    private void unregisterGlobalMouseListener() {
        if (globalMouseListener == null) {
            return;
        }
        Toolkit.getDefaultToolkit().removeAWTEventListener(globalMouseListener);
        globalMouseListener = null;
    }

    private boolean isComponentInsidePopup(Component component) {
        return SwingUtilities.isDescendingFrom(component, popup)
                || SwingUtilities.isDescendingFrom(component, popup.getRootPane());
    }

    private boolean isComponentInsideEditor(Component component) {
        if (SwingUtilities.isDescendingFrom(component, textArea)) {
            return true;
        }
        JScrollPane pane = (JScrollPane) SwingUtilities.getAncestorOfClass(JScrollPane.class, textArea);
        return pane != null && SwingUtilities.isDescendingFrom(component, pane);
    }

    private boolean isPopupVisible() {
        return popupVisibilityOverride != null ? popupVisibilityOverride : popup.isVisible();
    }

    private void setPopupVisible(boolean visible) {
        if (popupVisibilityOverride != null) {
            popupVisibilityOverride = visible;
            return;
        }
        popup.setVisible(visible);
    }

    void setPopupVisibilityForTest(boolean visible) {
        popupVisibilityOverride = visible;
    }

    String getLastHideReasonForTest() {
        return lastHideReason;
    }

    void clearLastHideReasonForTest() {
        lastHideReason = null;
    }

    void insertTextForTest(String text) {
        insertText(text);
    }

    /**
     * 可调整大小的联想弹窗，使用无边框 JDialog + 自定义边缘拖拽手柄。
     */
    private static class ResizableSuggestionDialog extends JDialog {
        private static final int RESIZE_MARGIN = 8;
        private static final int MIN_WIDTH = 240;
        private static final int MIN_HEIGHT = 120;
        private Dimension lastSize = new Dimension(320, 200);
        private Point dragStart;
        private Dimension dragInitialSize;
        private ResizeDirection dragDirection = ResizeDirection.NONE;

        ResizableSuggestionDialog(Window owner) {
            super(owner);
            setUndecorated(true);
            setResizable(true);
            setFocusableWindowState(false);
            setAlwaysOnTop(true);
            getRootPane().setBorder(new LineBorder(new Color(0xC0C0C0)));
            installResizeHandlers();
        }

        void showAt(Component invoker, Rectangle bounds) {
            if (!isVisible()) {
                setSize(lastSize);
            }
            Point screen = new Point(bounds.x, bounds.y + bounds.height);
            SwingUtilities.convertPointToScreen(screen, invoker);
            clampSizeToScreen();
            Rectangle usable = getScreenBounds();
            int x = Math.max(usable.x, Math.min(screen.x, usable.x + usable.width - getWidth()));
            int y = Math.max(usable.y, Math.min(screen.y, usable.y + usable.height - getHeight()));
            setLocation(x, y);
            setVisible(true);
        }

        @Override
        public void setSize(int width, int height) {
            int clampedWidth = Math.max(MIN_WIDTH, width);
            int clampedHeight = Math.max(MIN_HEIGHT, height);
            super.setSize(clampedWidth, clampedHeight);
            lastSize = getSize();
        }

        private void clampSizeToScreen() {
            Rectangle bounds = getScreenBounds();
            int width = Math.min(getWidth(), bounds.width);
            int height = Math.min(getHeight(), bounds.height);
            super.setSize(Math.max(MIN_WIDTH, width), Math.max(MIN_HEIGHT, height));
            lastSize = getSize();
        }

        private Rectangle getScreenBounds() {
            GraphicsConfiguration gc = getGraphicsConfiguration();
            if (gc != null) {
                return gc.getBounds();
            }
            Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
            return new Rectangle(0, 0, screen.width, screen.height);
        }

        private void installResizeHandlers() {
            MouseAdapter adapter = new MouseAdapter() {
                @Override
                public void mouseMoved(MouseEvent e) {
                    updateCursor(e.getPoint());
                }

                @Override
                public void mousePressed(MouseEvent e) {
                    dragDirection = hitTest(e.getPoint());
                    if (dragDirection != ResizeDirection.NONE) {
                        dragStart = e.getLocationOnScreen();
                        dragInitialSize = getSize();
                    }
                }

                @Override
                public void mouseDragged(MouseEvent e) {
                    if (dragDirection == ResizeDirection.NONE || dragStart == null || dragInitialSize == null) {
                        return;
                    }
                    Point current = e.getLocationOnScreen();
                    int dx = current.x - dragStart.x;
                    int dy = current.y - dragStart.y;

                    int targetWidth = dragInitialSize.width;
                    int targetHeight = dragInitialSize.height;
                    if (dragDirection.includesEast()) {
                        targetWidth += dx;
                    }
                    if (dragDirection.includesSouth()) {
                        targetHeight += dy;
                    }
                    setSize(targetWidth, targetHeight);
                    clampSizeToScreen();
                }

                @Override
                public void mouseReleased(MouseEvent e) {
                    dragDirection = ResizeDirection.NONE;
                    dragStart = null;
                    dragInitialSize = null;
                }
            };

            getRootPane().addMouseListener(adapter);
            getRootPane().addMouseMotionListener(adapter);
        }

        private void updateCursor(Point p) {
            ResizeDirection direction = hitTest(p);
            int cursor = switch (direction) {
                case EAST -> Cursor.E_RESIZE_CURSOR;
                case SOUTH -> Cursor.S_RESIZE_CURSOR;
                case SOUTH_EAST -> Cursor.SE_RESIZE_CURSOR;
                default -> Cursor.DEFAULT_CURSOR;
            };
            setCursor(Cursor.getPredefinedCursor(cursor));
        }

        private ResizeDirection hitTest(Point p) {
            boolean east = p.x >= getWidth() - RESIZE_MARGIN;
            boolean south = p.y >= getHeight() - RESIZE_MARGIN;
            if (east && south) return ResizeDirection.SOUTH_EAST;
            if (east) return ResizeDirection.EAST;
            if (south) return ResizeDirection.SOUTH;
            return ResizeDirection.NONE;
        }

        private enum ResizeDirection {
            NONE, EAST, SOUTH, SOUTH_EAST;

            boolean includesEast() {
                return this == EAST || this == SOUTH_EAST;
            }

            boolean includesSouth() {
                return this == SOUTH || this == SOUTH_EAST;
            }
        }
    }
}
