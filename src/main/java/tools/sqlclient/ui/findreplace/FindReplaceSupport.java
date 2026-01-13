package tools.sqlclient.ui.findreplace;

import javax.swing.*;
import javax.swing.text.JTextComponent;
import java.util.Objects;

public final class FindReplaceSupport {
    private static final String CLIENT_KEY = "findReplaceSupport.controller";

    private FindReplaceSupport() {
    }

    public static void install(JTextComponent editor, JComponent host) {
        Objects.requireNonNull(editor, "editor");
        Objects.requireNonNull(host, "host");
        if (editor.getClientProperty(CLIENT_KEY) instanceof FindReplaceController) {
            return;
        }
        FindReplaceController controller = new FindReplaceController(editor, host);
        controller.install();
        editor.putClientProperty(CLIENT_KEY, controller);
    }

    public static void toggleOrFocus(JTextComponent editor) {
        FindReplaceController controller = getController(editor);
        if (controller != null) {
            controller.toggleOrFocus();
        }
    }

    public static void close(JTextComponent editor) {
        FindReplaceController controller = getController(editor);
        if (controller != null) {
            controller.close();
        }
    }

    public static boolean isVisible(JTextComponent editor) {
        FindReplaceController controller = getController(editor);
        return controller != null && controller.isVisible();
    }

    private static FindReplaceController getController(JTextComponent editor) {
        if (editor == null) {
            return null;
        }
        Object value = editor.getClientProperty(CLIENT_KEY);
        if (value instanceof FindReplaceController controller) {
            return controller;
        }
        return null;
    }
}
