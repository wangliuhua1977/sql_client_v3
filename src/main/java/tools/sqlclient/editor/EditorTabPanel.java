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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
    private final Consumer<String> titleUpdater;
    private final NoteRepository noteRepository;
    private final Note note;
    private final FullWidthFilter fullWidthFilter;
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
        applyStyle(style);
        initLayout();
        LinkResolver.install(textArea);
        autoSaveService.startAutoSave(this::autoSave);
        updateTitle();
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
        area.setToolTipText("自动联想已开启");
        return area;
    }

    private void initLayout() {
        RTextScrollPane scrollPane = new RTextScrollPane(textArea);
        scrollPane.setFoldIndicatorEnabled(true);
        add(scrollPane, BorderLayout.CENTER);
        JPanel bottom = new JPanel(new FlowLayout(FlowLayout.LEFT));
        bottom.add(new JLabel("数据库: " + (databaseType == DatabaseType.POSTGRESQL ? "PostgreSQL" : "Hive")));
        bottom.add(lastSaveLabel);
        add(bottom, BorderLayout.SOUTH);
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
        scheme.getStyle(org.fife.ui.rsyntaxtextarea.Token.LITERAL_NULL).foreground = literal;
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
