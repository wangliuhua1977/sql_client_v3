package tools.sqlclient.editor;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;

/**
 * 笔记持久化策略，允许针对不同场景切换持久化/自动保存行为。
 */
public interface NotePersistenceStrategy {
    /**
     * 是否启用自动保存定时器。
     */
    default boolean shouldAutoSave() {
        return true;
    }

    /**
     * 是否允许执行保存/重命名等持久化动作。
     */
    default boolean isPersistenceEnabled() {
        return true;
    }

    /**
     * 是否需要在保存后持久化链接关系。
     */
    default boolean shouldPersistLinks() {
        return true;
    }

    /**
     * 保存当前内容。
     */
    default void save(NoteRepository repository, Note note, String content) {
        repository.updateContent(note, content);
    }

    /**
     * 重命名笔记。
     */
    default void rename(NoteRepository repository, Note note, String newTitle) {
        repository.rename(note, newTitle);
    }

    /**
     * 在 UI 上显示的自动保存提示前缀。
     */
    default String initialAutoSaveLabel() {
        return "自动保存: -";
    }
}
