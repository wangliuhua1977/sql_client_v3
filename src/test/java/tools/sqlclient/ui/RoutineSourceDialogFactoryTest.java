package tools.sqlclient.ui;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;

import javax.swing.JFrame;
import java.awt.GraphicsEnvironment;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class RoutineSourceDialogFactoryTest {

    @Test
    void openOwnedDialogBindsOwner() {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        NoteRepository noteRepository = mock(NoteRepository.class);
        MetadataService metadataService = mock(MetadataService.class);
        RoutineSourceDialogFactory factory = new RoutineSourceDialogFactory(noteRepository, metadataService, note -> {
        });
        Note note = new Note(-1L, "临时查看: test", "", DatabaseType.POSTGRESQL, 0L, 0L, "", false, false, 0L, "");
        EditorStyle style = new EditorStyle("default", 13,
                "#FFFFFF", "#000000", "#D0E7FF", "#000000",
                "#005CC5", "#22863A", "#6A737D", "#005CC5",
                "#6F42C1", "#6F42C1", "#E36209", "#24292E",
                "#032F62", "#FFFBCC", "#005CC5");
        JFrame owner = new JFrame("Main");
        TemporaryNoteWindow window = factory.openOwnedDialog(owner, note, style, false, 200, "leshan.test()");
        try {
            assertSame(owner, window.getOwner());
        } finally {
            window.dispose();
            owner.dispose();
        }
    }

    @Test
    void openOwnedDialogRejectsNullOwner() {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        NoteRepository noteRepository = mock(NoteRepository.class);
        MetadataService metadataService = mock(MetadataService.class);
        RoutineSourceDialogFactory factory = new RoutineSourceDialogFactory(noteRepository, metadataService, note -> {
        });
        Note note = new Note(-1L, "临时查看: test", "", DatabaseType.POSTGRESQL, 0L, 0L, "", false, false, 0L, "");
        EditorStyle style = new EditorStyle("default", 13,
                "#FFFFFF", "#000000", "#D0E7FF", "#000000",
                "#005CC5", "#22863A", "#6A737D", "#005CC5",
                "#6F42C1", "#6F42C1", "#E36209", "#24292E",
                "#032F62", "#FFFBCC", "#005CC5");

        assertThrows(IllegalStateException.class,
                () -> factory.openOwnedDialog(null, note, style, false, 200, "leshan.test()"));
    }
}
