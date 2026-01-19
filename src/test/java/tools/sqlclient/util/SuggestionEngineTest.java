package tools.sqlclient.util;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import tools.sqlclient.metadata.MetadataService;

import javax.swing.SwingUtilities;
import java.awt.GraphicsEnvironment;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SuggestionEngineTest {
    @Test
    void hidePopupIsIdempotent() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        RSyntaxTextArea textArea = new RSyntaxTextArea();
        SuggestionEngine engine = new SuggestionEngine(createMetadataService(), textArea);
        SwingUtilities.invokeAndWait(() -> {
            engine.setPopupVisibilityForTest(true);
            engine.hidePopup("first");
            engine.hidePopup("second");
            assertEquals("first", engine.getLastHideReasonForTest());
        });
    }

    @Test
    void caretDismissIsSuppressedDuringInsert() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        RSyntaxTextArea textArea = new RSyntaxTextArea();
        SuggestionEngine engine = new SuggestionEngine(createMetadataService(), textArea);
        SwingUtilities.invokeAndWait(() -> {
            textArea.setText("select ");
            textArea.setCaretPosition(textArea.getText().length());
            engine.setPopupVisibilityForTest(true);
            engine.clearLastHideReasonForTest();
            engine.insertTextForTest("table");
        });
        SwingUtilities.invokeAndWait(() -> {});
        assertNull(engine.getLastHideReasonForTest());
    }

    @Test
    void caretMoveDismissesPopup() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        RSyntaxTextArea textArea = new RSyntaxTextArea();
        SuggestionEngine engine = new SuggestionEngine(createMetadataService(), textArea);
        SwingUtilities.invokeAndWait(() -> {
            textArea.setText("select * from demo");
            textArea.setCaretPosition(textArea.getText().length());
            engine.setPopupVisibilityForTest(true);
            engine.clearLastHideReasonForTest();
            textArea.setCaretPosition(0);
        });
        SwingUtilities.invokeAndWait(() -> {});
        assertEquals("caretMoved", engine.getLastHideReasonForTest());
    }

    private MetadataService createMetadataService() throws Exception {
        Path dbPath = Files.createTempFile("sql-client-test", ".db");
        dbPath.toFile().deleteOnExit();
        return new MetadataService(dbPath);
    }
}
