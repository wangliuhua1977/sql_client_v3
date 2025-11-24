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
import tools.sqlclient.util.SuggestionEngine;

import javax.swing.*;
import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
    private final FullWidthFilter fullWidthFilter;
    private final JTabbedPane resultTabs = new JTabbedPane();
    private final JPanel resultWrapper = new JPanel(new BorderLayout());
    private final JToggleButton resultToggle = new JToggleButton();
    private JSplitPane splitPane;
    private int lastDividerLocation = -1;
    private javax.swing.Timer execTimer;
    private Runnable executeHandler;
    private EditorStyle currentStyle;
    private int runtimeFontSize;

    public EditorTabPanel(NoteRepository noteRepository, MetadataService metadataService,
                          java.util.function.Consumer<String> autosaveCallback,
                          java.util.function.IntConsumer taskCallback,
                          Consumer<String> titleUpdater,
                          Note note,
                          boolean convertFullWidth,
                          EditorStyle style) {
        super(new BorderLayout());
        this.noteRepository = noteRepository;
        this.note = note;
        this.databaseType = note.getDatabaseType();
        this.textArea = createEditor();
        this.currentStyle = style;
        this.runtimeFontSize = style.getFontSize();
        this.titleUpdater = titleUpdater;
        this.autoSaveService = new AutoSaveService(autosaveCallback, taskCallback);
        this.suggestionEngine = new SuggestionEngine(metadataService, textArea);
        this.fullWidthFilter = new FullWidthFilter(convertFullWidth);
        if (textArea.getDocument() instanceof javax.swing.text.AbstractDocument doc) {
            doc.setDocumentFilter(fullWidthFilter);
        }
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
        this.textArea.setText(note.getContent());
        installExecuteShortcut();
        applyStyle(style);
        initLayout();
        LinkResolver.install(textArea);
        autoSaveService.startAutoSave(this::autoSave);
        updateTitle();
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
        JPanel editorPanel = new JPanel(new BorderLayout());
        editorPanel.add(scrollPane, BorderLayout.CENTER);

        JPanel status = new JPanel(new FlowLayout(FlowLayout.LEFT));
        status.add(new JLabel("数据库: " + (databaseType == DatabaseType.POSTGRESQL ? "PostgreSQL" : "Hive")));
        status.add(lastSaveLabel);
        status.add(execStatusLabel);
        editorPanel.add(status, BorderLayout.SOUTH);

        JPanel togglePanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 2));
        resultToggle.setSelected(false);
        resultToggle.setMargin(new Insets(2, 6, 2, 6));
        resultToggle.addActionListener(e -> updateResultVisibility());
        togglePanel.add(resultToggle);
        resultTabs.setBorder(BorderFactory.createEmptyBorder());
        JScrollPane resultScroll = new JScrollPane(resultTabs);
        resultScroll.setPreferredSize(new Dimension(100, 220));
        resultWrapper.add(togglePanel, BorderLayout.NORTH);
        resultWrapper.add(resultScroll, BorderLayout.CENTER);
        resultWrapper.setMinimumSize(new Dimension(100, 120));

        splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, editorPanel, resultWrapper);
        splitPane.setResizeWeight(1.0);
        splitPane.setDividerSize(8);
        splitPane.setOneTouchExpandable(true);
        splitPane.setContinuousLayout(true);
        splitPane.setBorder(BorderFactory.createEmptyBorder());
        splitPane.addPropertyChangeListener(JSplitPane.DIVIDER_LOCATION_PROPERTY, evt -> {
            int height = splitPane.getHeight();
            int location = splitPane.getDividerLocation();
            int toggleHeight = resultToggle.getPreferredSize().height + 12;
            boolean open = location < height - toggleHeight;
            if (open != resultToggle.isSelected()) {
                resultToggle.setSelected(open);
                updateToggleLabel();
            }
            if (resultToggle.isSelected()) {
                lastDividerLocation = location;
            }
        });
        add(splitPane, BorderLayout.CENTER);
        SwingUtilities.invokeLater(this::collapseResultArea);
        updateToggleLabel();
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
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "保存失败: " + ex.getMessage());
        }
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

    public Note getNote() {
        return note;
    }

    public void setExecuteHandler(Runnable executeHandler) {
        this.executeHandler = executeHandler;
    }

    public java.util.List<String> getExecutableStatements(boolean blockMode) {
        String selected = textArea.getSelectedText();
        String target;
        if (selected != null && !selected.isBlank()) {
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

    private String extractStatementAtCaret() {
        String full = textArea.getText();
        int caret = textArea.getCaretPosition();
        int start = full.lastIndexOf(';', Math.max(0, caret - 1));
        start = start < 0 ? 0 : start + 1;
        int end = full.indexOf(';', caret);
        if (end < 0) end = full.length();
        return full.substring(start, Math.min(end, full.length()));
    }

    private String extractBlockAroundCaret() {
        String full = textArea.getText();
        int caret = Math.max(0, Math.min(full.length(), textArea.getCaretPosition()));
        int startBoundary = 0;
        int scan = caret;
        while (scan > 0) {
            int lineStart = full.lastIndexOf('\n', scan - 1) + 1;
            int lineEnd = full.indexOf('\n', lineStart);
            if (lineEnd < 0) lineEnd = full.length();
            String line = full.substring(lineStart, Math.min(lineEnd, full.length()));
            if (line.trim().isEmpty() && lineStart < caret) {
                startBoundary = Math.min(full.length(), lineEnd + 1);
                break;
            }
            if (lineStart == 0) {
                startBoundary = 0;
                break;
            }
            scan = Math.max(0, lineStart - 1);
        }

        int endBoundary = full.length();
        int scanDown = caret;
        while (scanDown < full.length()) {
            int lineEnd = full.indexOf('\n', scanDown);
            if (lineEnd < 0) lineEnd = full.length();
            String line = full.substring(scanDown, Math.min(lineEnd, full.length()));
            if (line.trim().isEmpty()) {
                endBoundary = Math.max(startBoundary, scanDown);
                break;
            }
            if (lineEnd >= full.length()) {
                endBoundary = full.length();
                break;
            }
            scanDown = lineEnd + 1;
        }
        if (endBoundary < startBoundary) {
            endBoundary = startBoundary;
        }
        return full.substring(startBoundary, endBoundary);
    }

    public JTabbedPane getResultTabs() {
        return resultTabs;
    }

    public void clearLocalResults() {
        resultTabs.removeAll();
        resultTabs.setVisible(false);
        resultToggle.setSelected(false);
        updateResultVisibility();
    }

    public void addLocalResultPanel(String title, JComponent panel, String hint) {
        resultTabs.addTab(title, panel);
        int idx = resultTabs.indexOfComponent(panel);
        if (idx >= 0) {
            resultTabs.setToolTipTextAt(idx, hint);
        }
        resultTabs.setVisible(true);
        resultTabs.setSelectedComponent(panel);
        resultToggle.setSelected(true);
        updateResultVisibility();
    }

    private void updateResultVisibility() {
        boolean show = resultToggle.isSelected();
        updateToggleLabel();
        adjustDividerForVisibility(show);
        resultWrapper.revalidate();
        resultWrapper.repaint();
    }

    private void adjustDividerForVisibility(boolean show) {
        if (splitPane == null) return;
        int minHeight = Math.max(resultToggle.getPreferredSize().height + 12, 32);
        if (show) {
            if (lastDividerLocation <= 0 || lastDividerLocation >= splitPane.getHeight()) {
                lastDividerLocation = (int) (splitPane.getHeight() * 0.7);
            }
            int target = Math.max(minHeight, Math.min(lastDividerLocation, splitPane.getHeight() - minHeight));
            SwingUtilities.invokeLater(() -> splitPane.setDividerLocation(target));
        } else {
            lastDividerLocation = splitPane.getDividerLocation();
            int collapsePos = Math.max(0, splitPane.getHeight() - minHeight);
            SwingUtilities.invokeLater(() -> splitPane.setDividerLocation(collapsePos));
        }
    }

    private void collapseResultArea() {
        adjustDividerForVisibility(false);
    }

    private void updateToggleLabel() {
        resultToggle.setText(resultToggle.isSelected() ? "▲" : "▼");
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
