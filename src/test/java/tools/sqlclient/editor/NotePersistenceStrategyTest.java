package tools.sqlclient.editor;

import org.junit.jupiter.api.Test;
import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.model.Note;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NotePersistenceStrategyTest {

    @Test
    void temporaryStrategyDoesNotPersistOrAutoSave() {
        RecordingRepository repo = new RecordingRepository();
        Note note = new Note(1, "temp", "", DatabaseType.POSTGRESQL, 0, 0, "", false, false, 0, "");

        TemporaryNotePersistenceStrategy strategy = new TemporaryNotePersistenceStrategy();
        strategy.save(repo, note, "content");
        strategy.rename(repo, note, "new");

        assertFalse(strategy.shouldAutoSave());
        assertFalse(strategy.isPersistenceEnabled());
        assertFalse(strategy.shouldPersistLinks());
        assertFalse(repo.saved);
        assertFalse(repo.renamed);
    }

    @Test
    void persistentStrategyWritesThroughRepository() {
        RecordingRepository repo = new RecordingRepository();
        Note note = new Note(2, "persistent", "", DatabaseType.POSTGRESQL, 0, 0, "", false, false, 0, "");

        PersistentNotePersistenceStrategy strategy = new PersistentNotePersistenceStrategy();
        strategy.save(repo, note, "content");
        strategy.rename(repo, note, "new");

        assertTrue(strategy.shouldAutoSave());
        assertTrue(strategy.isPersistenceEnabled());
        assertTrue(strategy.shouldPersistLinks());
        assertTrue(repo.saved);
        assertTrue(repo.renamed);
    }

    private static class RecordingRepository extends NoteRepository {
        boolean saved;
        boolean renamed;

        RecordingRepository() {
            super(null);
        }

        @Override
        public void updateContent(Note note, String content) {
            this.saved = true;
        }

        @Override
        public void rename(Note note, String newTitle) {
            this.renamed = true;
        }
    }
}
