package tools.sqlclient.editor;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rtextarea.RTextScrollPane;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;
import tools.sqlclient.util.AutoSaveService;
import tools.sqlclient.util.LinkResolver;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.SuggestionEngine;
import tools.sqlclient.util.ThreadPools;
import tools.sqlclient.util.SqlBlockDetector;

import javax.swing.*;
import javax.swing.text.BadLocationException;
import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 单个 SQL 标签面板，包含自动保存与联想逻辑。
 */
public class EditorTabPanel extends JPanel {
    private final DatabaseType databaseType;
    private final RSyntaxTextArea textArea;
    private final AutoSaveService autoSaveService;
    private final SuggestionEngine suggestionEngine;
    private final JLabel lastSaveLabel = new JLabel("自动保存: -");
    private final JLabel execStatusLabel = new JLabel("空闲");
    private final Consumer<String> titleUpdater;
    private final NoteRepository noteRepository;
    private final Note note;
    private final Consumer<String> linkOpener;
    private final FullWidthFilter fullWidthFilter;
    private final JTabbedPane resultTabs = new JTabbedPane();
    private final JPanel resultWrapper = new JPanel(new BorderLayout());
    private JPanel editorPanel;
    private JSplitPane splitPane;
    private int lastDividerLocation = -1;
    private javax.swing.Timer execTimer;
    private Runnable executeHandler;
    private EditorStyle currentStyle;
    private EditorStyle defaultStyle;
    private int runtimeFontSize;
    private final Consumer<EditorTabPanel> focusNotifier;
    private JMenu styleMenu;
    private List<EditorStyle> styleOptions = new ArrayList<>();
    private Consumer<EditorStyle> styleSelectionHandler;
    private Runnable resetStyleHandler;

    public EditorTabPanel(NoteRepository noteRepository, MetadataService metadataService,
                          java.util.function.Consumer<String> autosaveCallback,
                          java.util.function.IntConsumer taskCallback,
                          Consumer<String> titleUpdater,
                          Note note,
                          boolean convertFullWidth,
                          EditorStyle style,
                          Consumer<EditorTabPanel> focusNotifier,
                          Consumer<String> linkOpener) {
        super(new BorderLayout());
        this.noteRepository = noteRepository;
        this.note = note;
        this.databaseType = note.getDatabaseType();
        this.textArea = createEditor();
        this.currentStyle = style;
        this.defaultStyle = style;
        this.runtimeFontSize = style.getFontSize();
        this.titleUpdater = titleUpdater;
        this.linkOpener = linkOpener;
        this.autoSaveService = new AutoSaveService(autosaveCallback, taskCallback);
        this.suggestionEngine = new SuggestionEngine(metadataService, textArea);
        this.focusNotifier = focusNotifier;
        this.fullWidthFilter = new FullWidthFilter(convertFullWidth);
        if (textArea.getDocument() instanceof javax.swing.text.AbstractDocument doc) {
            doc.setDocumentFilter(fullWidthFilter);
        }
        installSmartParseMenu();
        this.textArea.addKeyListener(new java.awt.event.KeyAdapter() {
            @Override
            public void keyTyped(java.awt.event.KeyEvent e) {
                if (!fullWidthFilter.isEnabled()) return;
                char mapped = fullWidthFilter.normalizeChar(e.getKeyChar());
                if (mapped != e.getKeyChar()) {
                    e.consume();
                    textArea.replaceSelection(String.valueOf(mapped));
                }
            }
        });
        this.textArea.addKeyListener(suggestionEngine.createKeyListener());
        this.textArea.addMouseWheelListener(e -> {
            if (e.isShiftDown()) {
                runtimeFontSize = Math.max(10, Math.min(40, runtimeFontSize + (e.getWheelRotation() < 0 ? 1 : -1)));
                applyFontSize();
                e.consume();
            }
        });
        installFocusHooks();
        this.textArea.setText(note.getContent());
        installExecuteShortcut();
        applyStyle(style);
        initLayout();
        LinkResolver.install(textArea, this::handleLinkClick);
        autoSaveService.startAutoSave(this::autoSave);
        updateTitle();
    }

    private void handleLinkClick(LinkResolver.LinkRef ref) {
        if (ref == null || linkOpener == null) return;
        linkOpener.accept(ref.targetTitle);
    }

    private void installExecuteShortcut() {
        InputMap inputMap = textArea.getInputMap(JComponent.WHEN_FOCUSED);
        ActionMap actionMap = textArea.getActionMap();
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, InputEvent.CTRL_DOWN_MASK), "exec-block");
        actionMap.put("exec-block", new AbstractAction() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent e) {
                if (executeHandler != null) {
                    executeHandler.run();
                }
            }
        });
    }

    private void installFocusHooks() {
        java.awt.event.FocusAdapter adapter = new java.awt.event.FocusAdapter() {
            @Override
            public void focusGained(java.awt.event.FocusEvent e) {
                notifyFocus();
            }
        };
        textArea.addFocusListener(adapter);
        resultTabs.addFocusListener(adapter);
        this.addFocusListener(adapter);
    }

    private void installSmartParseMenu() {
        JPopupMenu popup = textArea.getPopupMenu();
        if (popup == null) {
            popup = new JPopupMenu();
            textArea.setPopupMenu(popup);
        }
        if (popup.getComponentCount() > 0) {
            popup.addSeparator();
        }
        JMenuItem smartParse = new JMenuItem("智能解析");
        smartParse.addActionListener(e -> performSmartParse());
        popup.add(smartParse);
    }

    private void installStyleMenu() {
        if (styleOptions == null || styleOptions.isEmpty() || defaultStyle == null) {
            return;
        }
        JPopupMenu popup = textArea.getPopupMenu();
        if (popup == null) {
            popup = new JPopupMenu();
            textArea.setPopupMenu(popup);
        }
        if (styleMenu != null) {
            popup.remove(styleMenu);
        }
        boolean followDefault = note.getStyleName() == null || note.getStyleName().isBlank();
        styleMenu = new JMenu("编辑器样式");
        ButtonGroup group = new ButtonGroup();
        JRadioButtonMenuItem follow = new JRadioButtonMenuItem("跟随全局默认", followDefault);
        follow.addActionListener(e -> {
            applyStyle(defaultStyle);
            if (resetStyleHandler != null) {
                resetStyleHandler.run();
            }
        });
        group.add(follow);
        styleMenu.add(follow);

        for (EditorStyle style : styleOptions) {
            boolean selected = !followDefault && style.getName().equalsIgnoreCase(note.getStyleName());
            JRadioButtonMenuItem item = new JRadioButtonMenuItem(style.getName(), selected);
            item.addActionListener(e -> {
                applyStyle(style);
                if (styleSelectionHandler != null) {
                    styleSelectionHandler.accept(style);
                }
            });
            group.add(item);
            styleMenu.add(item);
        }
        if (popup.getComponentCount() > 0 && !(popup.getComponent(popup.getComponentCount() - 1) instanceof JSeparator)) {
            popup.addSeparator();
        }
        popup.add(styleMenu);
    }

    private void performSmartParse() {
        String selection = textArea.getSelectedText();
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
        insertAfterSelectionLine(sql + System.lineSeparator());
    }

    private void insertAfterSelectionLine(String text) {
        try {
            int end = textArea.getSelectionEnd();
            int line = textArea.getLineOfOffset(end);
            int lineEnd = textArea.getLineEndOffset(line);
            textArea.getDocument().insertString(lineEnd, text, null);
            textArea.setCaretPosition(lineEnd + text.length());
        } catch (BadLocationException ex) {
            textArea.append(System.lineSeparator() + text);
            textArea.setCaretPosition(textArea.getText().length());
        }
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

    private RSyntaxTextArea createEditor() {
        RSyntaxTextArea area = new RSyntaxTextArea();
        area.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        area.setCodeFoldingEnabled(true);
        area.setAntiAliasingEnabled(true);
        area.setLineWrap(true);
        area.setWrapStyleWord(true);
        area.setMarkOccurrences(true);
        area.setAutoIndentEnabled(true);
        area.setTabSize(4);
        area.setFocusable(true);
        area.setToolTipText(null);
        return area;
    }

    private void initLayout() {
        RTextScrollPane scrollPane = new RTextScrollPane(textArea);
        scrollPane.setFoldIndicatorEnabled(true);
        editorPanel = new JPanel(new BorderLayout());
        editorPanel.add(scrollPane, BorderLayout.CENTER);

        JPanel status = new JPanel(new FlowLayout(FlowLayout.LEFT));
        status.add(new JLabel("数据库: " + (databaseType == DatabaseType.POSTGRESQL ? "PostgreSQL" : "Hive")));
        status.add(lastSaveLabel);
        status.add(execStatusLabel);
        editorPanel.add(status, BorderLayout.SOUTH);

        resultTabs.setBorder(BorderFactory.createEmptyBorder());
        JScrollPane resultScroll = new JScrollPane(resultTabs);
        resultScroll.setPreferredSize(new Dimension(100, 220));
        resultWrapper.add(resultScroll, BorderLayout.CENTER);
        resultWrapper.setMinimumSize(new Dimension(100, 160));
        resultWrapper.setVisible(false);

        editorPanel.setMinimumSize(new Dimension(120, 200));


        splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, editorPanel, resultWrapper);
        splitPane.setResizeWeight(1.0);
        splitPane.setDividerSize(10);
        splitPane.setOneTouchExpandable(false);
        splitPane.setContinuousLayout(true);
        splitPane.setBorder(BorderFactory.createEmptyBorder());
        splitPane.addPropertyChangeListener(JSplitPane.DIVIDER_LOCATION_PROPERTY, evt -> {
            if (splitPane.getHeight() <= 0) return;
            int minTop = editorPanel.getMinimumSize().height;
            int minBottom = resultWrapper.getMinimumSize().height;
            int location = Math.max(minTop, Math.min(splitPane.getDividerLocation(), splitPane.getHeight() - minBottom));
            lastDividerLocation = location;
        });
        add(splitPane, BorderLayout.CENTER);
        SwingUtilities.invokeLater(this::collapseResultArea);
    }

    public void saveNow() {
        saveInternal(true);
    }

    private void autoSave() {
        saveInternal(false);
    }

    private void saveInternal(boolean notify) {
        try {
            noteRepository.updateContent(note, textArea.getText());
            String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            lastSaveLabel.setText("自动保存: " + time);
            autoSaveService.onSaved(time);
            updateTitle();
            persistLinksAsync();
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "保存失败: " + ex.getMessage());
        }
    }

    private void persistLinksAsync() {
        String content = textArea.getText();
        ThreadPools.NETWORK_POOL.submit(() -> {
            try {
                LinkResolver.resolveAndPersistLinks(noteRepository, note, content);
            } catch (Exception ex) {
                OperationLog.log("解析/持久化链接失败: " + ex.getMessage());
            }
        });
    }

    private void updateTitle() {
        if (titleUpdater != null) {
            titleUpdater.accept(note.getTitle());
        }
    }

    public void rename(String newTitle) {
        try {
            noteRepository.rename(note, newTitle);
            updateTitle();
        } catch (IllegalArgumentException ex) {
            JOptionPane.showMessageDialog(this, ex.getMessage());
        }
    }

    public void setFullWidthConversionEnabled(boolean enabled) {
        fullWidthFilter.setEnabled(enabled);
    }

    public void refreshStyleMenu(List<EditorStyle> styles, EditorStyle globalDefault,
                                 Consumer<EditorStyle> selectionHandler, Runnable resetHandler) {
        this.styleOptions = new ArrayList<>(styles);
        this.styleSelectionHandler = selectionHandler;
        this.resetStyleHandler = resetHandler;
        this.defaultStyle = globalDefault;
        installStyleMenu();
    }

    public void applyStyle(EditorStyle style) {
        this.currentStyle = style;
        this.runtimeFontSize = style.getFontSize();
        applyFontSize();
        textArea.setBackground(Color.decode(style.getBackground()));
        textArea.setForeground(Color.decode(style.getForeground()));
        textArea.setCaretColor(Color.decode(style.getCaret()));
        textArea.setSelectionColor(Color.decode(style.getSelection()));
        var scheme = textArea.getSyntaxScheme();
        Color keyword = Color.decode(style.getKeyword());
        Color stringColor = Color.decode(style.getStringColor());
        Color commentColor = Color.decode(style.getCommentColor());
        Color numberColor = Color.decode(style.getNumberColor());
        Color operatorColor = Color.decode(style.getOperatorColor());
        Color functionColor = Color.decode(style.getFunctionColor());
        Color dataType = Color.decode(style.getDataTypeColor());
        Color identifier = Color.decode(style.getIdentifierColor());
        Color literal = Color.decode(style.getLiteralColor());
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.RESERVED_WORD).foreground = keyword;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.DATA_TYPE).foreground = dataType;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.FUNCTION).foreground = functionColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_STRING_DOUBLE_QUOTE).foreground = stringColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_CHAR).foreground = stringColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_BOOLEAN).foreground = literal;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_NUMBER_DECIMAL_INT).foreground = numberColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_NUMBER_FLOAT).foreground = numberColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_NUMBER_HEXADECIMAL).foreground = numberColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.OPERATOR).foreground = operatorColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.IDENTIFIER).foreground = identifier;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.COMMENT_EOL).foreground = commentColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.COMMENT_MULTILINE).foreground = commentColor;
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.COMMENT_DOCUMENTATION).foreground = commentColor;
        textArea.setCurrentLineHighlightColor(Color.decode(style.getLineHighlight()));
        textArea.setMatchedBracketBGColor(Color.decode(style.getBracketColor()));
        textArea.setMatchedBracketBorderColor(Color.decode(style.getBracketColor()));
        textArea.revalidate();
        textArea.repaint();
    }

    private void applyFontSize() {
        textArea.setFont(textArea.getFont().deriveFont((float) runtimeFontSize));
    }

    private void notifyFocus() {
        if (focusNotifier != null) {
            focusNotifier.accept(this);
        }
    }

    public Note getNote() {
        return note;
    }

    public void setExecuteHandler(Runnable executeHandler) {
        this.executeHandler = executeHandler;
    }

    public void setSuggestionEnabled(boolean enabled) {
        suggestionEngine.setEnabled(enabled);
    }

    public void hideSuggestionPopup() {
        suggestionEngine.closePopup();
    }

    public void revealMatch(int offset, String keyword) {
        String text = textArea.getText();
        if (text == null) {
            return;
        }
        int target = Math.max(0, Math.min(offset, text.length()));
        if (target >= text.length() && keyword != null && !keyword.isBlank()) {
            int idx = text.toLowerCase().indexOf(keyword.toLowerCase());
            if (idx >= 0) {
                target = idx;
            }
        }
        int end = target;
        if (keyword != null && !keyword.isBlank()) {
            end = Math.min(text.length(), target + keyword.length());
        }
        textArea.requestFocusInWindow();
        textArea.setCaretPosition(target);
        textArea.select(target, end);
        try {
            Rectangle view = textArea.modelToView(target);
            textArea.scrollRectToVisible(view);
        } catch (Exception ignored) {
        }
    }

    public java.util.List<String> getExecutableStatements(boolean blockMode) {
        String selected = textArea.getSelectedText();
        String target;
        if (selected != null && !selected.isBlank()) {
            SqlBlockDetector.DdlBlockDetectionResult detectionResult = SqlBlockDetector.detectSingleDdlBlock(selected);
            if (detectionResult.isSingleDdlBlock()) {
                java.util.List<String> list = new java.util.ArrayList<>();
                list.add(selected);
                return list;
            }
            target = selected;
        } else if (blockMode) {
            target = extractBlockAroundCaret();
        } else {
            target = extractStatementAtCaret();
        }
        java.util.List<String> list = new java.util.ArrayList<>();
        for (String part : target.split(";")) {
            if (!part.trim().isEmpty()) {
                list.add(part.trim());
            }
        }
        return list;
    }

    public java.util.List<String> getExecutableStatements() {
        return getExecutableStatements(false);
    }

    public void insertSqlAtCaret(String sql) {
        if (sql == null || sql.isBlank()) {
            return;
        }
        int start = textArea.getSelectionStart();
        int end = textArea.getSelectionEnd();
        textArea.requestFocusInWindow();
        textArea.replaceRange(sql, start, end);
        textArea.setCaretPosition(start + sql.length());
    }

    private String extractStatementAtCaret() {
        String full = textArea.getText();
        int caret = textArea.getCaretPosition();
        caret = Math.max(0, Math.min(full.length(), caret));
        int start = full.lastIndexOf(';', Math.max(0, caret - 1));
        start = start < 0 ? 0 : start + 1;
        int end = full.indexOf(';', caret);
        if (end < 0) end = full.length();
        return full.substring(start, Math.min(end, full.length()));
    }

    /**
     * 以“当前行”为种子，向上/向下扩散，遇到空白行或文件边界停止，
     * 返回中间所有非空行组成的整块文本。
     */
    private String extractBlockAroundCaret() {
        String full = textArea.getText();
        if (full == null || full.isEmpty()) {
            return "";
        }

        int caret = textArea.getCaretPosition();
        // caret 可能在文本末尾（等于 length），需要回退一格才能安全取行号
        if (caret > 0 && caret >= full.length()) {
            caret = full.length() - 1;
        }

        try {
            int currentLine = textArea.getLineOfOffset(caret);
            int startLine = currentLine;
            int endLine = currentLine;

            // 向上扩散：遇到空白行就停
            for (int line = currentLine - 1; line >= 0; line--) {
                int lineStart = textArea.getLineStartOffset(line);
                int lineEnd = textArea.getLineEndOffset(line);
                String lineText = full.substring(lineStart, Math.min(lineEnd, full.length()));
                if (lineText.trim().isEmpty()) {
                    break;
                }
                startLine = line;
            }

            // 向下扩散：遇到空白行就停
            int totalLines = textArea.getLineCount();
            for (int line = currentLine + 1; line < totalLines; line++) {
                int lineStart = textArea.getLineStartOffset(line);
                int lineEnd = textArea.getLineEndOffset(line);
                String lineText = full.substring(lineStart, Math.min(lineEnd, full.length()));
                if (lineText.trim().isEmpty()) {
                    break;
                }
                endLine = line;
            }

            int startOffset = textArea.getLineStartOffset(startLine);
            int endOffset = textArea.getLineEndOffset(endLine);
            endOffset = Math.min(endOffset, full.length());
            if (endOffset < startOffset) {
                endOffset = startOffset;
            }
            return full.substring(startOffset, endOffset);
        } catch (javax.swing.text.BadLocationException e) {
            // 出现异常时退化为整篇文本，保证不会崩
            return full;
        }
    }

    public JTabbedPane getResultTabs() {
        return resultTabs;
    }

    public void clearLocalResults() {
        resultTabs.removeAll();
        resultTabs.setVisible(false);
        resultWrapper.setVisible(false);
        collapseResultArea();
    }

    public void addLocalResultPanel(String title, JComponent panel, String hint) {
        resultTabs.addTab(title, panel);
        int idx = resultTabs.indexOfComponent(panel);
        if (idx >= 0) {
            resultTabs.setToolTipTextAt(idx, hint);
        }
        resultTabs.setVisible(true);
        resultTabs.setSelectedComponent(panel);
        resultWrapper.setVisible(true);
        expandResultArea();
    }

    private void expandResultArea() {
        if (splitPane == null) return;
        int height = splitPane.getHeight();
        int minTop = editorPanel.getMinimumSize().height;
        int minBottom = resultWrapper.getMinimumSize().height;
        int target = lastDividerLocation > 0 ? lastDividerLocation : (int) (height * 0.65);
        int maxLocation = Math.max(minTop, height - minBottom);
        int clamped = Math.max(minTop, Math.min(target, maxLocation));
        SwingUtilities.invokeLater(() -> splitPane.setDividerLocation(clamped));
    }

    private void collapseResultArea() {
        if (splitPane == null) return;
        int height = splitPane.getHeight();
        int minTop = editorPanel.getMinimumSize().height;
        int minBottom = resultWrapper.getMinimumSize().height;
        int collapsePos = resultWrapper.isVisible() ? Math.max(minTop, height - minBottom) : height;
        SwingUtilities.invokeLater(() -> splitPane.setDividerLocation(collapsePos));
    }

    public void setExecutionRunning(boolean running) {
        if (running) {
            execStatusLabel.setText("执行中...");
            if (execTimer == null) {
                execTimer = new javax.swing.Timer(250, null);
                execTimer.addActionListener(new java.awt.event.ActionListener() {
                    private int step = 0;

                    @Override
                    public void actionPerformed(java.awt.event.ActionEvent e) {
                        String dots = switch (step % 4) {
                            case 0 -> "·  ";
                            case 1 -> "·· ";
                            case 2 -> "···";
                            default -> " ··";
                        };
                        execStatusLabel.setText("执行中 " + dots);
                        step++;
                    }
                });
            }
            if (!execTimer.isRunning()) {
                execTimer.start();
            }
        } else {
            execStatusLabel.setText("空闲");
            if (execTimer != null && execTimer.isRunning()) {
                execTimer.stop();
            }
        }
    }

    /**
     * 将常见中文全角符号转为半角，避免 SQL 语法错误。
     */
    private static class FullWidthFilter extends javax.swing.text.DocumentFilter {
        private volatile boolean enabled;

        FullWidthFilter(boolean enabled) {
            this.enabled = enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public void insertString(FilterBypass fb, int offset, String string, javax.swing.text.AttributeSet attr) throws javax.swing.text.BadLocationException {
            super.insertString(fb, offset, convert(string), attr);
        }

        @Override
        public void replace(FilterBypass fb, int offset, int length, String text, javax.swing.text.AttributeSet attrs) throws javax.swing.text.BadLocationException {
            super.replace(fb, offset, length, convert(text), attrs);
        }

        private String convert(String input) {
            if (!enabled || input == null) return input;
            StringBuilder sb = new StringBuilder(input.length());
            for (char ch : input.toCharArray()) {
                sb.append(normalizeChar(ch));
            }
            return sb.toString();
        }

        public char normalizeChar(char ch) {
            return switch (ch) {
                case '，' -> ',';
                case '。' -> '.';
                case '（' -> '(';
                case '）' -> ')';
                case '｛' -> '{';
                case '｝' -> '}';
                case '【' -> '[';
                case '】' -> ']';
                case '；' -> ';';
                case '：' -> ':';
                case '“', '”' -> '"';
                case '‘', '’' -> '\'';
                case '《' -> '<';
                case '》' -> '>';
                case '、' -> ',';
                default -> ch;
            };
        }
    }
}
