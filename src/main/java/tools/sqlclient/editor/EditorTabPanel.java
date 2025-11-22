package tools.sqlclient.editor;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rtextarea.RTextScrollPane;
import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.model.DatabaseType;
import tools.sqlclient.util.AutoSaveService;
import tools.sqlclient.util.LinkResolver;
import tools.sqlclient.util.SuggestionEngine;

import javax.swing.*;
import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单个 SQL 标签面板，包含自动保存与联想逻辑。
 */
public class EditorTabPanel extends JPanel {
    private final DatabaseType databaseType;
    private final RSyntaxTextArea textArea;
    private final Path filePath;
    private final AutoSaveService autoSaveService;
    private final SuggestionEngine suggestionEngine;
    private final AtomicBoolean ctrlSpaceEnabled = new AtomicBoolean(true);
    private final JLabel lastSaveLabel = new JLabel("自动保存: -");

    public EditorTabPanel(DatabaseType databaseType, MetadataService metadataService,
                          java.util.function.Consumer<String> autosaveCallback,
                          java.util.function.IntConsumer taskCallback) {
        super(new BorderLayout());
        this.databaseType = databaseType;
        this.textArea = createEditor();
        this.filePath = Path.of("notes", databaseType.name().toLowerCase() + "_" + UUID.randomUUID() + ".sql");
        this.autoSaveService = new AutoSaveService(autosaveCallback, taskCallback);
        this.suggestionEngine = new SuggestionEngine(metadataService, textArea);
        initLayout();
        LinkResolver.install(textArea);
        autoSaveService.startAutoSave(this::saveNow);
    }

    private RSyntaxTextArea createEditor() {
        RSyntaxTextArea area = new RSyntaxTextArea();
        area.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        area.setCodeFoldingEnabled(true);
        area.setAntiAliasingEnabled(true);
        area.setLineWrap(true);
        area.setWrapStyleWord(true);
        area.setMarkOccurrences(true);
        area.setAutoIndentEnabled(true);
        area.setTabSize(4);
        area.setFocusable(true);
        area.setToolTipText("Ctrl+Space 开关自动联想");
        area.addKeyListener(suggestionEngine.createKeyListener(ctrlSpaceEnabled));
        return area;
    }

    private void initLayout() {
        RTextScrollPane scrollPane = new RTextScrollPane(textArea);
        scrollPane.setFoldIndicatorEnabled(true);
        add(scrollPane, BorderLayout.CENTER);
        JPanel bottom = new JPanel(new FlowLayout(FlowLayout.LEFT));
        bottom.add(new JLabel("数据库: " + (databaseType == DatabaseType.POSTGRESQL ? "PostgreSQL" : "Hive")));
        bottom.add(lastSaveLabel);
        add(bottom, BorderLayout.SOUTH);
    }

    public void saveNow() {
        try {
            Files.createDirectories(filePath.getParent());
            Files.writeString(filePath, textArea.getText(), StandardCharsets.UTF_8);
            String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            lastSaveLabel.setText("自动保存: " + time);
            autoSaveService.onSaved(time);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "保存失败: " + ex.getMessage());
        }
    }

    public Optional<Path> getFilePath() {
        return Optional.of(filePath);
    }
}
