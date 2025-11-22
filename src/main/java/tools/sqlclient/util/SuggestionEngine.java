package tools.sqlclient.util;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import tools.sqlclient.metadata.MetadataService;

import javax.swing.*;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 基于 SQLite 元数据的模糊联想。
 */
public class SuggestionEngine {
    private final MetadataService metadataService;
    private final RSyntaxTextArea textArea;
    private final JPopupMenu popup = new JPopupMenu();
    private final JList<String> list = new JList<>();

    public SuggestionEngine(MetadataService metadataService, RSyntaxTextArea textArea) {
        this.metadataService = metadataService;
        this.textArea = textArea;
        popup.setFocusable(false);
        popup.add(new JScrollPane(list));
        list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        list.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting() && list.getSelectedValue() != null) {
                insertText(list.getSelectedValue());
                popup.setVisible(false);
            }
        });
    }

    public KeyAdapter createKeyListener(AtomicBoolean ctrlSpaceEnabled) {
        return new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
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
                if (shouldTrigger(e)) {
                    String prefix = currentToken();
                    showSuggestions(prefix);
                } else {
                    popup.setVisible(false);
                }
            }
        };
    }

    private boolean shouldTrigger(KeyEvent e) {
        char ch = e.getKeyChar();
        return Character.isAlphabetic(ch) || ch == '%' || ch == '.' || e.getKeyCode() == KeyEvent.VK_SPACE;
    }

    private String currentToken() {
        int caret = textArea.getCaretPosition();
        try {
            int start = Math.max(textArea.getText(0, caret).lastIndexOf(' ') + 1, 0);
            int lastNewLine = textArea.getText(0, caret).lastIndexOf('\n') + 1;
            start = Math.max(start, lastNewLine);
            int length = caret - start;
            return textArea.getText(start, length).trim();
        } catch (Exception e) {
            return "";
        }
    }

    private void showSuggestions(String token) {
        if (token.isEmpty()) {
            popup.setVisible(false);
            return;
        }
        List<String> suggestions = metadataService.fuzzyMatch(token, 15);
        if (suggestions.isEmpty()) {
            popup.setVisible(false);
            return;
        }
        list.setListData(suggestions.toArray(String[]::new));
        try {
            Rectangle view = textArea.modelToView(textArea.getCaretPosition());
            popup.show(textArea, view.x, view.y + view.height);
        } catch (Exception ignored) {
            popup.setVisible(false);
        }
    }

    private void insertText(String text) {
        try {
            int start = textArea.getSelectionStart();
            int end = textArea.getSelectionEnd();
            textArea.getDocument().remove(start, end - start);
            textArea.getDocument().insertString(start, text, null);
        } catch (Exception ignored) {
        }
    }
}
