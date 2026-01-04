package tools.sqlclient.editor;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;

/**
 * 临时笔记模式：不触发任何持久化与自动保存。
 */
public class TemporaryNotePersistenceStrategy implements NotePersistenceStrategy {
    @Override
    public boolean shouldAutoSave() {
        return false;
    }

    @Override
    public boolean isPersistenceEnabled() {
        return false;
    }

    @Override
    public void save(NoteRepository repository, Note note, String content) {
        // no-op
    }

    @Override
    public void rename(NoteRepository repository, Note note, String newTitle) {
        // no-op
    }

    @Override
    public boolean shouldPersistLinks() {
        return false;
    }

    @Override
    public String initialAutoSaveLabel() {
        return "自动保存: 已禁用（临时）";
    }
}
