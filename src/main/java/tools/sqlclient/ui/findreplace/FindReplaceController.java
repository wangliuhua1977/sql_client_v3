package tools.sqlclient.ui.findreplace;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.Document;
import javax.swing.text.Highlighter;
import javax.swing.text.JTextComponent;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FindReplaceController {
    private static final int HIGHLIGHT_LIMIT = 800;
    private static final int DEBOUNCE_MS = 200;
    private static final Highlighter.HighlightPainter ALL_PAINTER =
            new DefaultHighlighter.DefaultHighlightPainter(new Color(252, 240, 178));
    private static final Highlighter.HighlightPainter CURRENT_PAINTER =
            new DefaultHighlighter.DefaultHighlightPainter(new Color(255, 196, 117));

    private final JTextComponent editor;
    private final JComponent host;
    private final FindReplaceEngine engine = new FindReplaceEngine();
    private final FindReplacePanel panel = new FindReplacePanel();
    private final Timer refreshTimer;
    private final DocumentListener docListener;
    private final List<Object> highlightTags = new ArrayList<>();
    private Object currentTag;
    private FindReplaceEngine.FindAllResult lastAllResult = new FindReplaceEngine.FindAllResult(List.of(), 0, false, null);
    private String lastQuery = "";
    private FindOptions lastOptions = new FindOptions("", false, false, false, true, FindOptions.Direction.FORWARD);
    private boolean disposed;

    public FindReplaceController(JTextComponent editor, JComponent host) {
        this.editor = Objects.requireNonNull(editor, "editor");
        this.host = Objects.requireNonNull(host, "host");
        this.refreshTimer = new Timer(DEBOUNCE_MS, e -> refreshHighlights());
        this.refreshTimer.setRepeats(false);
        this.docListener = new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                scheduleRefresh();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                scheduleRefresh();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                scheduleRefresh();
            }
        };
    }

    public void install() {
        if (!(host.getLayout() instanceof BorderLayout)) {
            host.setLayout(new BorderLayout());
        }
        host.add(panel, BorderLayout.SOUTH);
        panel.setVisible(false);
        editor.getDocument().addDocumentListener(docListener);
        installPanelActions();
        installKeyBindings();
        editor.addHierarchyListener(e -> {
            if ((e.getChangeFlags() & java.awt.event.HierarchyEvent.DISPLAYABILITY_CHANGED) != 0
                    && !editor.isDisplayable()) {
                dispose();
            }
        });
    }

    public void toggleOrFocus() {
        if (disposed) {
            return;
        }
        boolean showing = panel.isVisible();
        if (!showing) {
            panel.setVisible(true);
            String selection = editor.getSelectedText();
            if ((selection != null && !selection.isBlank()) && panel.getSearchField().getText().isBlank()) {
                panel.getSearchField().setText(selection);
            }
            refreshHighlights();
        }
        panel.focusSearchField();
    }

    public void close() {
        if (disposed) {
            return;
        }
        panel.setVisible(false);
        clearHighlights();
        refreshTimer.stop();
        panel.setStatus(" ");
        editor.requestFocusInWindow();
    }

    public boolean isVisible() {
        return panel.isVisible();
    }

    public void dispose() {
        if (disposed) {
            return;
        }
        disposed = true;
        refreshTimer.stop();
        clearHighlights();
        editor.getDocument().removeDocumentListener(docListener);
        host.remove(panel);
    }

    private void installPanelActions() {
        panel.getPrevButton().addActionListener(e -> findPrevious());
        panel.getNextButton().addActionListener(e -> findNext());
        panel.getReplaceButton().addActionListener(e -> replaceCurrent());
        panel.getReplaceFindButton().addActionListener(e -> replaceAndFind());
        panel.getReplaceAllButton().addActionListener(e -> replaceAll());
        panel.getCloseButton().addActionListener(e -> close());

        panel.getSearchField().getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                scheduleRefresh();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                scheduleRefresh();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                scheduleRefresh();
            }
        });

        panel.getCaseCheck().addActionListener(e -> refreshHighlights());
        panel.getWholeCheck().addActionListener(e -> refreshHighlights());
        panel.getRegexCheck().addActionListener(e -> refreshHighlights());
        panel.getWrapCheck().addActionListener(e -> refreshHighlights());
    }

    private void installKeyBindings() {
        InputMap inputMap = editor.getInputMap(JComponent.WHEN_FOCUSED);
        ActionMap actionMap = editor.getActionMap();
        inputMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_F, java.awt.event.InputEvent.CTRL_DOWN_MASK),
                "find_replace.toggle");
        actionMap.put("find_replace.toggle", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                toggleOrFocus();
            }
        });

        inputMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_F3, 0), "find_replace.next");
        inputMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_F3,
                java.awt.event.InputEvent.SHIFT_DOWN_MASK), "find_replace.prev");
        actionMap.put("find_replace.next", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                findNext();
            }
        });
        actionMap.put("find_replace.prev", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                findPrevious();
            }
        });

        inputMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ESCAPE, 0), "find_replace.close_if_visible");
        actionMap.put("find_replace.close_if_visible", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (panel.isVisible()) {
                    close();
                    return;
                }
                JRootPane rootPane = SwingUtilities.getRootPane(editor);
                if (rootPane != null) {
                    Action action = rootPane.getActionMap().get("stop_execution_if_running");
                    if (action != null) {
                        action.actionPerformed(e);
                    }
                }
            }
        });

        InputMap searchMap = panel.getSearchField().getInputMap(JComponent.WHEN_FOCUSED);
        ActionMap searchActionMap = panel.getSearchField().getActionMap();
        searchMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ENTER, 0), "find_replace.next");
        searchMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ENTER,
                java.awt.event.InputEvent.SHIFT_DOWN_MASK), "find_replace.prev");
        searchMap.put(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_ESCAPE, 0), "find_replace.close");
        searchActionMap.put("find_replace.next", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                findNext();
            }
        });
        searchActionMap.put("find_replace.prev", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                findPrevious();
            }
        });
        searchActionMap.put("find_replace.close", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                close();
            }
        });
    }

    private void scheduleRefresh() {
        if (!panel.isVisible()) {
            return;
        }
        refreshTimer.restart();
    }

    private void refreshHighlights() {
        if (!panel.isVisible()) {
            return;
        }
        String query = panel.getSearchField().getText();
        if (query == null || query.isBlank()) {
            clearHighlights();
            panel.setStatus("请输入查找内容");
            return;
        }
        FindOptions options = buildOptions(FindOptions.Direction.FORWARD);
        String text = getEditorText();
        if (text == null) {
            return;
        }
        FindReplaceEngine.FindAllResult result = engine.findAll(text, options, HIGHLIGHT_LIMIT);
        if (result.errorMessage() != null) {
            clearHighlights();
            panel.setStatus("正则错误: " + result.errorMessage());
            lastAllResult = result;
            return;
        }
        lastAllResult = result;
        lastQuery = query;
        lastOptions = options;
        applyHighlights(result);
    }

    private void applyHighlights(FindReplaceEngine.FindAllResult result) {
        clearHighlights();
        if (result.total() <= 0) {
            panel.setStatus("未找到");
            return;
        }
        if (result.tooMany()) {
            panel.setStatus("匹配过多，仅定位当前");
            highlightCurrentFromCaret();
            return;
        }
        Highlighter highlighter = editor.getHighlighter();
        for (FindReplaceEngine.Match match : result.matches()) {
            try {
                Object tag = highlighter.addHighlight(match.start(), match.end(), ALL_PAINTER);
                highlightTags.add(tag);
            } catch (BadLocationException ignored) {
                return;
            }
        }
        highlightCurrentFromCaret();
    }

    private void highlightCurrentFromCaret() {
        FindResult current = findInternal(true);
        if (current != null && current.found()) {
            highlightCurrent(current.start(), current.end());
            updateStatusWithIndex(current.start(), current.end(), current.wrapped());
        }
    }

    private void highlightCurrent(int start, int end) {
        Highlighter highlighter = editor.getHighlighter();
        if (currentTag != null) {
            highlighter.removeHighlight(currentTag);
        }
        try {
            currentTag = highlighter.addHighlight(start, end, CURRENT_PAINTER);
        } catch (BadLocationException ignored) {
            currentTag = null;
        }
    }

    private void clearHighlights() {
        Highlighter highlighter = editor.getHighlighter();
        for (Object tag : highlightTags) {
            highlighter.removeHighlight(tag);
        }
        highlightTags.clear();
        if (currentTag != null) {
            highlighter.removeHighlight(currentTag);
            currentTag = null;
        }
    }

    private void findNext() {
        if (!panel.isVisible()) {
            toggleOrFocus();
            return;
        }
        FindResult result = findInternal(true);
        handleFindResult(result, true);
    }

    private void findPrevious() {
        if (!panel.isVisible()) {
            toggleOrFocus();
            return;
        }
        FindResult result = findInternal(false);
        handleFindResult(result, false);
    }

    private FindResult findInternal(boolean forward) {
        String query = panel.getSearchField().getText();
        if (query == null || query.isBlank()) {
            panel.setStatus("请输入查找内容");
            return FindResult.notFound(false);
        }
        String text = getEditorText();
        if (text == null) {
            return FindResult.notFound(false);
        }
        int startPos = forward ? editor.getSelectionEnd() : Math.max(0, editor.getSelectionStart());
        if (!forward && startPos > 0) {
            startPos = startPos - 1;
        }
        FindOptions.Direction direction = forward ? FindOptions.Direction.FORWARD : FindOptions.Direction.BACKWARD;
        FindOptions options = buildOptions(direction);
        return engine.find(text, startPos, options);
    }

    private void handleFindResult(FindResult result, boolean forward) {
        if (result == null) {
            return;
        }
        if (result.errorMessage() != null) {
            panel.setStatus("正则错误: " + result.errorMessage());
            return;
        }
        if (!result.found()) {
            if (!buildOptions(FindOptions.Direction.FORWARD).wrap()) {
                panel.setStatus(forward ? "已到末尾" : "已到开头");
            } else {
                panel.setStatus("未找到");
            }
            return;
        }
        selectMatch(result.start(), result.end());
        highlightCurrent(result.start(), result.end());
        updateStatusWithIndex(result.start(), result.end(), result.wrapped());
    }

    private void updateStatusWithIndex(int start, int end, boolean wrapped) {
        if (lastAllResult.errorMessage() != null) {
            panel.setStatus("正则错误: " + lastAllResult.errorMessage());
            return;
        }
        if (lastAllResult.tooMany()) {
            panel.setStatus("匹配过多，仅定位当前");
            return;
        }
        if (!Objects.equals(panel.getSearchField().getText(), lastQuery)
                || !optionsCompatible(lastOptions, buildOptions(FindOptions.Direction.FORWARD))) {
            refreshHighlights();
        }
        int index = -1;
        List<FindReplaceEngine.Match> matches = lastAllResult.matches();
        for (int i = 0; i < matches.size(); i++) {
            FindReplaceEngine.Match match = matches.get(i);
            if (match.start() == start && match.end() == end) {
                index = i + 1;
                break;
            }
        }
        if (index > 0) {
            panel.setStatus("第 " + index + "/" + matches.size() + " 个匹配" + (wrapped ? " (已回绕)" : ""));
        } else {
            panel.setStatus(wrapped ? "已回绕" : "已定位");
        }
    }

    private boolean optionsCompatible(FindOptions a, FindOptions b) {
        return Objects.equals(a.query(), b.query())
                && a.caseSensitive() == b.caseSensitive()
                && a.wholeWord() == b.wholeWord()
                && a.regex() == b.regex()
                && a.wrap() == b.wrap();
    }

    private void selectMatch(int start, int end) {
        editor.setSelectionStart(start);
        editor.setSelectionEnd(end);
        try {
            Rectangle rect = editor.modelToView2D(start).getBounds();
            editor.scrollRectToVisible(rect);
        } catch (BadLocationException ignored) {
        }
    }

    private void replaceCurrent() {
        panel.setReplaceVisible(true);
        if (!replaceSelectionIfMatch()) {
            FindResult result = findInternal(true);
            if (result.found()) {
                selectMatch(result.start(), result.end());
                highlightCurrent(result.start(), result.end());
                replaceSelectionIfMatch();
            } else {
                handleFindResult(result, true);
            }
        }
        refreshHighlights();
    }

    private void replaceAndFind() {
        panel.setReplaceVisible(true);
        if (replaceSelectionIfMatch()) {
            findNext();
        } else {
            FindResult result = findInternal(true);
            handleFindResult(result, true);
            if (result.found()) {
                replaceSelectionIfMatch();
                findNext();
            }
        }
    }

    private void replaceAll() {
        panel.setReplaceVisible(true);
        String query = panel.getSearchField().getText();
        if (query == null || query.isBlank()) {
            panel.setStatus("请输入查找内容");
            return;
        }
        String text = getEditorText();
        if (text == null) {
            return;
        }
        FindOptions options = buildOptions(FindOptions.Direction.FORWARD);
        FindReplaceEngine.FindAllResult result = engine.findAll(text, options, Integer.MAX_VALUE - 1);
        if (result.errorMessage() != null) {
            panel.setStatus("正则错误: " + result.errorMessage());
            return;
        }
        if (result.total() == 0) {
            panel.setStatus("未找到");
            return;
        }
        boolean atomic = beginAtomicEdit();
        int replaced = 0;
        Document doc = editor.getDocument();
        List<FindReplaceEngine.Match> matches = result.matches();
        for (int i = matches.size() - 1; i >= 0; i--) {
            FindReplaceEngine.Match match = matches.get(i);
            try {
                doc.remove(match.start(), match.end() - match.start());
                doc.insertString(match.start(), panel.getReplaceField().getText(), null);
                replaced++;
            } catch (BadLocationException ignored) {
                break;
            }
        }
        endAtomicEdit(atomic);
        panel.setStatus("已替换 " + replaced + " 处");
        refreshHighlights();
    }

    private boolean replaceSelectionIfMatch() {
        String selection = editor.getSelectedText();
        if (selection == null || selection.isEmpty()) {
            return false;
        }
        FindOptions options = buildOptions(FindOptions.Direction.FORWARD);
        if (!isSelectionMatch(selection, options)) {
            return false;
        }
        editor.replaceSelection(panel.getReplaceField().getText());
        return true;
    }

    private boolean isSelectionMatch(String selection, FindOptions options) {
        FindReplaceEngine engineLocal = engine;
        String text = selection;
        FindReplaceEngine.FindAllResult result = engineLocal.findAll(text, options, 2);
        if (result.errorMessage() != null) {
            panel.setStatus("正则错误: " + result.errorMessage());
            return false;
        }
        return result.total() == 1 && result.matches().get(0).start() == 0
                && result.matches().get(0).end() == selection.length();
    }

    private boolean beginAtomicEdit() {
        if (editor instanceof RSyntaxTextArea area) {
            area.beginAtomicEdit();
            return true;
        }
        return false;
    }

    private void endAtomicEdit(boolean atomic) {
        if (atomic && editor instanceof RSyntaxTextArea area) {
            area.endAtomicEdit();
        }
    }

    private FindOptions buildOptions(FindOptions.Direction direction) {
        return new FindOptions(
                panel.getSearchField().getText(),
                panel.getCaseCheck().isSelected(),
                panel.getWholeCheck().isSelected(),
                panel.getRegexCheck().isSelected(),
                panel.getWrapCheck().isSelected(),
                direction
        );
    }

    private String getEditorText() {
        Document doc = editor.getDocument();
        try {
            return doc.getText(0, doc.getLength());
        } catch (BadLocationException ex) {
            return null;
        }
    }
}
