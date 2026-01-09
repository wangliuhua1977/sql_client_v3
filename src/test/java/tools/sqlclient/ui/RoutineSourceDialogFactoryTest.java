package tools.sqlclient.ui;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import java.awt.GraphicsEnvironment;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class RoutineSourceDialogFactoryTest {

    @Test
    void openOwnedDialogRequiresOwner() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "Headless environment skips Swing window test.");

        RoutineSourceDialogFactory factory = new RoutineSourceDialogFactory(
                mock(NoteRepository.class),
                mock(MetadataService.class),
                note -> {
                });
        Note note = new Note(1L, "temp", "", DatabaseType.POSTGRESQL, 0L, 0L, "", false, false, 0L, "");
        EditorStyle style = new EditorStyle("default", 12, "#FFFFFF", "#000000", "#CCCCCC", "#000000",
                "#000000", "#000000", "#000000", "#000000", "#000000", "#000000", "#000000",
                "#000000", "#000000", "#FFFFFF", "#000000");

        SwingUtilities.invokeAndWait(() -> assertThrows(IllegalStateException.class, () ->
                factory.openOwnedDialog(null, note, style, true, 200, "test")));
    }

    @Test
    void openOwnedDialogBindsOwner() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "Headless environment skips Swing window test.");

        RoutineSourceDialogFactory factory = new RoutineSourceDialogFactory(
                mock(NoteRepository.class),
                mock(MetadataService.class),
                note -> {
                });
        Note note = new Note(1L, "temp", "", DatabaseType.POSTGRESQL, 0L, 0L, "", false, false, 0L, "");
        EditorStyle style = new EditorStyle("default", 12, "#FFFFFF", "#000000", "#CCCCCC", "#000000",
                "#000000", "#000000", "#000000", "#000000", "#000000", "#000000", "#000000",
                "#000000", "#000000", "#FFFFFF", "#000000");

        JFrame frame = new JFrame("owner");
        try {
            SwingUtilities.invokeAndWait(() -> {
                TemporaryNoteWindow window = factory.openOwnedDialog(frame, note, style, true, 200, "test");
                try {
                    assertSame(frame, window.getOwner());
                } finally {
                    window.dispose();
                }
            });
        } finally {
            frame.dispose();
        }
    }
}
