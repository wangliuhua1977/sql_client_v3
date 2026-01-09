package tools.sqlclient.ui;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;

import java.awt.Dialog;
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
        return openRoutineSourceDialog(owner, temporaryNote, style, convertFullWidth, defaultPageSize, routineDisplayName, false);
    }

    public TemporaryNoteWindow openRoutineSourceDialog(Window owner,
                                                       Note temporaryNote,
                                                       EditorStyle style,
                                                       boolean convertFullWidth,
                                                       int defaultPageSize,
                                                       String routineDisplayName,
                                                       boolean readOnly) {
        if (owner == null) {
            log.error("Routine source dialog requires owner. routine={} readOnly={}", routineDisplayName, readOnly);
            throw new IllegalStateException("owner must not be null for routine source dialog");
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
                routineDisplayName,
                readOnly);
        if (window.getOwner() == null || window.getOwner() != owner) {
            log.error("Routine source dialog owner mismatch routine={} expectedOwner={}#{} actualOwner={}#{}",
                    routineDisplayName,
                    owner.getClass().getName(),
                    System.identityHashCode(owner),
                    window.getOwner() == null ? "null" : window.getOwner().getClass().getName(),
                    window.getOwner() == null ? "null" : System.identityHashCode(window.getOwner()));
            throw new IllegalStateException("routine source dialog must be owned");
        }
        String ownerTitle = resolveOwnerTitle(owner);
        log.info("Open routine source dialog routine={} readOnly={} title={} owner={}#{} ownerTitle={}",
                routineDisplayName,
                readOnly,
                window.getTitle(),
                owner.getClass().getName(),
                System.identityHashCode(owner),
                ownerTitle);
        window.bindOwnerVisibility(owner);
        return window;
    }

    private String resolveOwnerTitle(Window owner) {
        if (owner instanceof Frame) {
            return ((Frame) owner).getTitle();
        }
        if (owner instanceof Dialog) {
            return ((Dialog) owner).getTitle();
        }
        return owner.getName();
    }
}
