package tools.sqlclient.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;

import java.awt.Frame;
import java.awt.Window;
import java.util.function.Consumer;

public class RoutineSourceDialogFactory {
    private static final Logger log = LoggerFactory.getLogger(RoutineSourceDialogFactory.class);
    private final NoteRepository noteRepository;
    private final MetadataService metadataService;
    private final Consumer<Note> permanentNoteOpener;

    public RoutineSourceDialogFactory(NoteRepository noteRepository,
                                      MetadataService metadataService,
                                      Consumer<Note> permanentNoteOpener) {
        this.noteRepository = noteRepository;
        this.metadataService = metadataService;
        this.permanentNoteOpener = permanentNoteOpener;
    }

    public TemporaryNoteWindow openOwnedDialog(Window owner,
                                               Note temporaryNote,
                                               EditorStyle style,
                                               boolean convertFullWidth,
                                               int defaultPageSize,
                                               String routineDisplayName) {
        if (owner == null) {
            log.error("Routine source window must be owned, resolve owner failed for routine={}", routineDisplayName);
            throw new IllegalStateException("Routine source window must be owned by a main window.");
        }
        TemporaryNoteWindow window = new TemporaryNoteWindow(
                owner,
                noteRepository,
                metadataService,
                temporaryNote,
                style,
                convertFullWidth,
                defaultPageSize,
                permanentNoteOpener,
                routineDisplayName);
        if (window.getOwner() == null || window.getOwner() != owner) {
            log.error("Routine source window owner mismatch routine={} expectedOwner={}#{} actualOwner={}#{}",
                    routineDisplayName,
                    owner.getClass().getName(),
                    System.identityHashCode(owner),
                    window.getOwner() == null ? "null" : window.getOwner().getClass().getName(),
                    window.getOwner() == null ? "null" : System.identityHashCode(window.getOwner()));
            throw new IllegalStateException("Routine source window must be owned by a main window.");
        }
        String ownerTitle = owner instanceof Frame ? ((Frame) owner).getTitle() : owner.getName();
        log.info("Open routine source window routine={} title={} owner={}#{} ownerTitle={}",
                routineDisplayName,
                window.getTitle(),
                owner.getClass().getName(),
                System.identityHashCode(owner),
                ownerTitle);
        window.bindOwnerVisibility(owner);
        return window;
    }
}
