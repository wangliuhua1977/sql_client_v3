package tools.sqlclient.ui;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.editor.EditorTabPanel;
import tools.sqlclient.editor.TemporaryNotePersistenceStrategy;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.EditorStyle;
import tools.sqlclient.model.Note;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * 存储过程/函数定义的临时查看窗口，不做持久化，允许用户显式另存为永久笔记。
 */
public class TemporaryNoteWindow extends JFrame {
    private final NoteRepository noteRepository;
    private final Note temporaryNote;
    private final Consumer<Note> permanentNoteOpener;
    private final EditorTabPanel editorPanel;

    public TemporaryNoteWindow(Frame owner,
                               NoteRepository noteRepository,
                               MetadataService metadataService,
                               Note temporaryNote,
                               EditorStyle style,
                               boolean convertFullWidth,
                               int defaultPageSize,
                               Consumer<Note> permanentNoteOpener,
                               String routineDisplayName) {
        super("临时查看：" + routineDisplayName);
        this.noteRepository = noteRepository;
        this.temporaryNote = temporaryNote;
        this.permanentNoteOpener = permanentNoteOpener;
        setLayout(new BorderLayout(8, 8));
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);

        editorPanel = new EditorTabPanel(
                noteRepository,
                metadataService,
                t -> {},
                i -> {},
                title -> {},
                temporaryNote,
                convertFullWidth,
                style,
                p -> {},
                s -> {},
                defaultPageSize,
                new TemporaryNotePersistenceStrategy()
        );
        editorPanel.setBorder(new EmptyBorder(4, 4, 4, 4));
        JPanel toolbar = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton saveButton = new JButton("保存为永久笔记…");
        saveButton.addActionListener(e -> saveAsPermanent());
        toolbar.add(saveButton);

        add(toolbar, BorderLayout.NORTH);
        add(editorPanel, BorderLayout.CENTER);
        setSize(960, 720);
        setLocationRelativeTo(owner);
    }

    public EditorTabPanel getEditorPanel() {
        return editorPanel;
    }

    private void saveAsPermanent() {
        String suggestion = buildSuggestedTitle();
        String title = JOptionPane.showInputDialog(this, "保存为永久笔记", suggestion);
        if (title == null) {
            return;
        }
        title = title.trim();
        if (title.isEmpty()) {
            JOptionPane.showMessageDialog(this, "标题不能为空");
            return;
        }
        try {
            Note created = noteRepository.create(title, temporaryNote.getDatabaseType());
            noteRepository.updateContent(created, editorPanel.getSqlText());
            if (permanentNoteOpener != null) {
                permanentNoteOpener.accept(created);
            }
            dispose();
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "保存失败: " + ex.getMessage());
        }
    }

    private String buildSuggestedTitle() {
        String base = temporaryNote.getTitle();
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return base + "_" + ts;
    }
}
