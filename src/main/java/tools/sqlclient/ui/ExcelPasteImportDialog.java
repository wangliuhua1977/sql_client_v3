package tools.sqlclient.ui;

import javax.swing.BorderFactory;
import javax.swing.DefaultCellEditor;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.KeyStroke;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.DataFlavor;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import tools.sqlclient.exec.DatabaseErrorInfo;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionException;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.util.OperationLog;

/**
 * 支持粘贴 Excel/TSV 内容并导入 PostgreSQL 的对话框。
 */
public class ExcelPasteImportDialog extends JDialog {
    private static final int PREVIEW_ROWS = 20;
    private static final int INFERENCE_SAMPLE_LIMIT = 5000;
    private static final int DEFAULT_BATCH_ROWS = 200;
    private static final int MAX_SQL_SIZE = 512 * 1024; // 512KB
    private static final int MAX_CLIPBOARD_LENGTH = 20 * 1024 * 1024; // 20MB
    private static final int MAX_HTML_LENGTH = 5 * 1024 * 1024; // 避免超大 HTML

    private final SqlExecutionService sqlExecutionService;
    private final JTextArea pasteArea = new JTextArea();
    private final JTextArea pastePreviewArea = new JTextArea();
    private final JTable previewTable = new JTable();
    private final JTable fieldTable = new JTable();
    private final JLabel statusLabel = new JLabel("请粘贴包含表头的数据");
    private final JLabel progressText = new JLabel("等待粘贴");
    private final JProgressBar progressBar = new JProgressBar();
    private final JTextField tableNameField = new JTextField();
    private final javax.swing.JComboBox<String> schemaCombo = new javax.swing.JComboBox<>();
    private final javax.swing.JComboBox<String> strategyCombo = new javax.swing.JComboBox<>();
    private final JToggleButton fieldToggle = new JToggleButton("字段设置");
    private final JPanel fieldPanel = new JPanel(new BorderLayout());
    private final AtomicBoolean parsing = new AtomicBoolean(false);
    private final AtomicBoolean importing = new AtomicBoolean(false);
    private Path tempFile;
    private List<ColumnMeta> columns = new ArrayList<>();
    private SwingWorker<?, ?> currentWorker;
    private int sampledTotalRows;
    private SampleResult lastSample;
    private boolean typeNormalizedNotified;

    public ExcelPasteImportDialog(JFrame owner, SqlExecutionService service) {
        super(owner, "粘贴导入", true);
        this.sqlExecutionService = service;
        setPreferredSize(new Dimension(1100, 720));
        setLayout(new BorderLayout());
        initUI();
        registerPasteShortcut();
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                cleanupTemp();
                cancelCurrentWorker();
            }
        });
        pack();
        setLocationRelativeTo(owner);
    }

    private void initUI() {
        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 8));
        top.setBorder(new EmptyBorder(8, 8, 4, 8));
        JLabel schemaLabel = new JLabel("Schema:");
        schemaCombo.setEditable(true);
        schemaCombo.setModel(new DefaultComboBoxModel<>(new String[]{"leshan", "public"}));
        schemaCombo.setSelectedIndex(0);
        JLabel tableLabel = new JLabel("Table:");
        tableNameField.setColumns(20);
        tableNameField.setText("tmp_wlh_import_1");
        JLabel strategyLabel = new JLabel("导入策略:");
        strategyCombo.setModel(new DefaultComboBoxModel<>(new String[]{"自动适配", "直接追加", "清空插入", "删除后重建"}));
        statusLabel.setFont(statusLabel.getFont().deriveFont(Font.PLAIN, 12f));
        statusLabel.setForeground(java.awt.Color.DARK_GRAY);
        top.add(schemaLabel);
        top.add(schemaCombo);
        top.add(tableLabel);
        top.add(tableNameField);
        top.add(strategyLabel);
        top.add(strategyCombo);
        top.add(statusLabel);
        add(top, BorderLayout.NORTH);

        JSplitPane centerSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        centerSplit.setResizeWeight(0.45);
        pasteArea.setLineWrap(true);
        pasteArea.setWrapStyleWord(false);
        pasteArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        pasteArea.setBorder(BorderFactory.createTitledBorder("粘贴区域 (Ctrl+V)"));
        pastePreviewArea.setEditable(false);
        pastePreviewArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        pastePreviewArea.setBorder(BorderFactory.createTitledBorder("粘贴内容预览 (前" + PREVIEW_ROWS + "行)"));
        JPanel leftPanel = new JPanel(new BorderLayout());
        leftPanel.add(new JScrollPane(pastePreviewArea), BorderLayout.NORTH);
        leftPanel.add(new JScrollPane(pasteArea), BorderLayout.CENTER);
        centerSplit.setLeftComponent(leftPanel);

        previewTable.setModel(new DefaultTableModel());
        previewTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        JScrollPane previewScroll = new JScrollPane(previewTable);
        previewScroll.setBorder(BorderFactory.createTitledBorder("预览 (仅前" + PREVIEW_ROWS + "行)"));
        centerSplit.setRightComponent(previewScroll);

        add(centerSplit, BorderLayout.CENTER);

        fieldPanel.setVisible(false);
        fieldPanel.setBorder(BorderFactory.createEmptyBorder(4, 8, 4, 8));
        fieldTable.setModel(new DefaultTableModel(new Object[][]{}, new String[]{"导入", "源列", "目标字段名", "目标类型"}) {
            @Override
            public Class<?> getColumnClass(int columnIndex) {
                return switch (columnIndex) {
                    case 0 -> Boolean.class;
                    default -> String.class;
                };
            }

            @Override
            public boolean isCellEditable(int row, int column) {
                return column != 1;
            }
        });
        fieldTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        TableColumn importCol = fieldTable.getColumnModel().getColumn(0);
        importCol.setCellEditor(new DefaultCellEditor(new JCheckBox()));
        TableColumn typeCol = fieldTable.getColumnModel().getColumn(3);
        typeCol.setCellEditor(new DefaultCellEditor(new javax.swing.JComboBox<>(allowedTypes())));
        JScrollPane fieldScroll = new JScrollPane(fieldTable);
        fieldPanel.add(fieldScroll, BorderLayout.CENTER);

        JPanel fieldHeader = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 4));
        fieldToggle.addActionListener(e -> fieldPanel.setVisible(fieldToggle.isSelected()));
        JButton resetInferenceBtn = new JButton(new javax.swing.AbstractAction("重置推断") {
            @Override
            public void actionPerformed(ActionEvent e) {
                applyColumns(columns);
            }
        });
        fieldHeader.add(fieldToggle);
        fieldHeader.add(resetInferenceBtn);
        JPanel fieldWrapper = new JPanel(new BorderLayout());
        fieldWrapper.add(fieldHeader, BorderLayout.NORTH);
        fieldWrapper.add(fieldPanel, BorderLayout.CENTER);
        add(fieldWrapper, BorderLayout.SOUTH);

        JPanel bottom = new JPanel(new BorderLayout());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 8));
        javax.swing.JButton importBtn = new javax.swing.JButton("导入");
        javax.swing.JButton cancelBtn = new javax.swing.JButton("取消");
        javax.swing.JButton resetBtn = new javax.swing.JButton("重置");
        buttons.add(importBtn);
        buttons.add(resetBtn);
        buttons.add(cancelBtn);
        progressBar.setStringPainted(true);
        progressText.setHorizontalAlignment(SwingConstants.LEFT);
        JPanel progressPanel = new JPanel(new BorderLayout());
        progressPanel.setBorder(new EmptyBorder(0, 8, 4, 8));
        progressPanel.add(progressBar, BorderLayout.CENTER);
        progressPanel.add(progressText, BorderLayout.SOUTH);
        bottom.add(progressPanel, BorderLayout.CENTER);
        bottom.add(buttons, BorderLayout.EAST);
        add(bottom, BorderLayout.PAGE_END);

        cancelBtn.addActionListener(e -> {
            cancelCurrentWorker();
            progressText.setText("已取消");
        });
        importBtn.addActionListener(e -> startImport());
        resetBtn.addActionListener(e -> resetAll());
    }

    private void registerPasteShortcut() {
        JMenuItem pasteItem = new JMenuItem(new javax.swing.AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                handlePaste();
            }
        });
        pasteItem.setAccelerator(KeyStroke.getKeyStroke("ctrl V"));
        pasteArea.getInputMap().put(pasteItem.getAccelerator(), "paste-import");
        pasteArea.getActionMap().put("paste-import", pasteItem.getAction());
    }

    private void handlePaste() {
        if (parsing.get()) {
            return;
        }
        parsing.set(true);
        progressText.setText("读取剪贴板...");
        SwingWorker<Void, Void> worker = new SwingWorker<>() {
            private String error;
            @Override
            protected Void doInBackground() {
                try {
                    ClipboardPayload payload = readClipboard();
                    if (payload == null) {
                        error = "剪贴板无可用文本";
                        return null;
                    }
                    cleanupTemp();
                    tempFile = Files.createTempFile("paste-import", ".tsv");
                    tempFile.toFile().deleteOnExit();
                    try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
                        payload.writeAsTsv(writer);
                    }
                    long sizeKb = Files.size(tempFile) / 1024;
                    SwingUtilities.invokeLater(() -> pasteArea.setText("已保存到临时文件 (~" + sizeKb + " KB)，右侧展示预览"));
                    progressText.setText("已写入临时文件，开始采样...");
                    startSampling();
                } catch (IOException ex) {
                    error = ex.getMessage();
                }
                return null;
            }

            @Override
            protected void done() {
                parsing.set(false);
                if (error != null) {
                    progressText.setText("读取失败: " + error);
                    JOptionPane.showMessageDialog(ExcelPasteImportDialog.this, error, "粘贴失败", JOptionPane.ERROR_MESSAGE);
                }
            }
        };
        currentWorker = worker;
        worker.execute();
    }

    private ClipboardPayload readClipboard() throws IOException {
        Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
        if (clipboard == null) {
            return null;
        }
        try {
            if (clipboard.isDataFlavorAvailable(DataFlavor.allHtmlFlavor)) {
                String html = (String) clipboard.getData(DataFlavor.allHtmlFlavor);
                if (html != null && html.length() > MAX_CLIPBOARD_LENGTH) {
                    throw new IOException("HTML 粘贴内容过大，超过 " + (MAX_CLIPBOARD_LENGTH / 1024 / 1024) + "MB");
                }
                if (html != null && html.length() <= MAX_HTML_LENGTH) {
                    OperationLog.log("检测到 HTML 粘贴");
                    return ClipboardPayload.fromHtml(html);
                }
            }
            if (clipboard.isDataFlavorAvailable(DataFlavor.stringFlavor)) {
                String text = (String) clipboard.getData(DataFlavor.stringFlavor);
                if (text != null && text.length() > MAX_CLIPBOARD_LENGTH) {
                    throw new IOException("剪贴板文本过大，超过 " + (MAX_CLIPBOARD_LENGTH / 1024 / 1024) + "MB");
                }
                return ClipboardPayload.fromTsv(text);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return null;
    }

    private void startSampling() {
        if (tempFile == null) {
            progressText.setText("暂无临时文件");
            return;
        }
        SwingWorker<SampleResult, Void> worker = new SwingWorker<>() {
            @Override
            protected SampleResult doInBackground() throws Exception {
                return sampleFile(tempFile);
            }

            @Override
            protected void done() {
                try {
                    SampleResult result = get();
                    applySample(result);
                } catch (Exception e) {
                    progressText.setText("解析失败: " + e.getMessage());
                    OperationLog.log("采样解析失败: " + e.getMessage());
                }
            }
        };
        currentWorker = worker;
        worker.execute();
    }

    private SampleResult sampleFile(Path file) throws IOException {
        List<List<String>> preview = new ArrayList<>();
        List<ColumnStats> stats = new ArrayList<>();
        int totalRows = 0;
        int maxCols = 0;
        Set<String> usedNames = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            int rowIndex = 0;
            List<String> header = null;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t", -1);
                maxCols = Math.max(maxCols, parts.length);
                if (header == null) {
                    header = normalizeHeader(parts);
                    usedNames.addAll(header);
                    stats = initStats(header.size());
                    preview.add(Arrays.asList(parts));
                    continue;
                }
                if (parts.length > header.size()) {
                    for (int i = header.size(); i < parts.length; i++) {
                        String newName = normalizeColumnName("col_" + (i + 1), usedNames);
                        usedNames.add(newName);
                        header.add(newName);
                        stats.add(new ColumnStats());
                    }
                }
                List<String> row = new ArrayList<>(header.size());
                for (int i = 0; i < header.size(); i++) {
                    String v = i < parts.length ? parts[i] : "";
                    row.add(v);
                    if (rowIndex < INFERENCE_SAMPLE_LIMIT) {
                        stats.get(i).accept(v);
                    }
                }
                if (preview.size() < PREVIEW_ROWS) {
                    preview.add(row);
                }
                rowIndex++;
                totalRows++;
            }
            if (header == null) {
                throw new IOException("数据为空");
            }
            List<ColumnMeta> metas = buildColumns(header, stats);
            return new SampleResult(preview, metas, totalRows, maxCols);
        }
    }

    private void applySample(SampleResult result) {
        this.columns = result.columns();
        this.lastSample = result;
        this.sampledTotalRows = result.totalRows();
        this.typeNormalizedNotified = false;
        statusLabel.setText("Rows=" + result.totalRows() + " Cols=" + result.maxCols());
        progressText.setText("已完成采样，推断字段类型");
        applyPreview(result.preview());
        applyPastePreview(result.preview());
        applyColumns(result.columns());
        suggestTableName();
        progressBar.setIndeterminate(false);
        progressBar.setMinimum(0);
        progressBar.setMaximum(Math.max(1, sampledTotalRows));
        progressBar.setValue(0);
    }

    private void applyPreview(List<List<String>> data) {
        if (data.isEmpty()) {
            previewTable.setModel(new DefaultTableModel());
            return;
        }
        List<String> header = new ArrayList<>();
        for (ColumnMeta col : columns) {
            header.add(col.sourceName() + " (" + col.inferredType() + ")");
        }
        DefaultTableModel model = new DefaultTableModel(header.toArray(), 0);
        for (int i = 1; i < data.size(); i++) {
            List<String> row = data.get(i);
            Object[] arr = new Object[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                arr[c] = c < row.size() ? row.get(c) : "";
            }
            model.addRow(arr);
        }
        previewTable.setModel(model);
    }

    private void applyPastePreview(List<List<String>> data) {
        if (data.isEmpty()) {
            pastePreviewArea.setText("");
            return;
        }
        StringBuilder sb = new StringBuilder();
        int rows = Math.min(PREVIEW_ROWS, data.size());
        for (int i = 0; i < rows; i++) {
            List<String> row = data.get(i);
            for (int c = 0; c < columns.size(); c++) {
                if (c > 0) sb.append(" | ");
                sb.append(c < row.size() ? row.get(c) : "");
            }
            if (i < rows - 1) {
                sb.append('\n');
            }
        }
        pastePreviewArea.setText(sb.toString());
    }

    private void applyColumns(List<ColumnMeta> cols) {
        DefaultTableModel model = new DefaultTableModel(new Object[][]{}, new String[]{"导入", "源列", "目标字段名", "目标类型"}) {
            @Override
            public Class<?> getColumnClass(int columnIndex) {
                return switch (columnIndex) {
                    case 0 -> Boolean.class;
                    default -> String.class;
                };
            }

            @Override
            public boolean isCellEditable(int row, int column) {
                return column != 1;
            }
        };
        for (ColumnMeta c : cols) {
            model.addRow(new Object[]{Boolean.TRUE, c.sourceName(), c.targetName(), normalizeType(c.inferredType())});
        }
        model.addTableModelListener(e -> {
            if (e.getType() == javax.swing.event.TableModelEvent.UPDATE && e.getColumn() == 3) {
                int row = e.getFirstRow();
                String raw = Objects.toString(model.getValueAt(row, 3), "text");
                String normalized = normalizeType(raw);
                if (!normalized.equals(raw)) {
                    model.setValueAt(normalized, row, 3);
                    notifyTypeNormalized();
                }
            }
        });
        fieldTable.setModel(model);
        TableColumn importCol = fieldTable.getColumnModel().getColumn(0);
        importCol.setCellEditor(new DefaultCellEditor(new JCheckBox()));
        TableColumn typeCol = fieldTable.getColumnModel().getColumn(3);
        typeCol.setCellEditor(new DefaultCellEditor(new javax.swing.JComboBox<>(allowedTypes())));
    }

    private List<ColumnStats> initStats(int size) {
        List<ColumnStats> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(new ColumnStats());
        }
        return list;
    }

    private List<String> normalizeHeader(String[] headers) {
        List<String> names = new ArrayList<>();
        Set<String> used = new HashSet<>();
        int index = 1;
        for (String h : headers) {
            String base = h == null || h.isBlank() ? "col_" + index : h;
            String normalized = normalizeColumnName(base, used);
            used.add(normalized);
            names.add(normalized);
            index++;
        }
        return names;
    }

    private String normalizeColumnName(String raw, Set<String> used) {
        String name = raw.trim().toLowerCase(Locale.ROOT);
        name = Normalizer.normalize(name, Normalizer.Form.NFKC);
        name = name.replaceAll("[^a-z0-9_]", "_");
        if (name.isEmpty()) {
            name = "col";
        }
        if (Character.isDigit(name.charAt(0))) {
            name = "c_" + name;
        }
        if (isReserved(name)) {
            name = "c_" + name;
        }
        String candidate = name;
        int counter = 2;
        while (used.contains(candidate)) {
            candidate = name + "_" + counter;
            counter++;
        }
        return candidate;
    }

    private boolean isReserved(String name) {
        Set<String> reserved = Set.of("user", "select", "table", "from", "where", "group", "order", "limit", "offset", "join", "insert", "update", "delete");
        return reserved.contains(name);
    }

    private List<ColumnMeta> buildColumns(List<String> header, List<ColumnStats> stats) {
        List<ColumnMeta> metas = new ArrayList<>();
        for (int i = 0; i < header.size(); i++) {
            ColumnStats s = stats.get(i);
            String type = s.inferType();
            metas.add(new ColumnMeta(header.get(i), header.get(i), type, i));
        }
        return metas;
    }

    private void suggestTableName() {
        String base = tableNameField.getText();
        String schema = Objects.toString(schemaCombo.getSelectedItem(), "public");
        String preferred = ensureImportSuffix(schema, base);
        tableNameField.setText(preferred);
        statusLabel.setText("Rows=" + sampledTotalRows + " Cols=" + columns.size() + " | 最终表名: " + preferred);
    }

    private String resolveAvailableName(String schema, String base) {
        String sanitized = base.trim();
        String candidate = sanitized;
        int counter = 1;
        while (tableExists(schema, candidate)) {
            candidate = sanitized + "_" + counter;
            counter++;
        }
        return candidate;
    }

    private String ensureImportSuffix(String schema, String raw) {
        String base = raw == null || raw.isBlank() ? "tmp_wlh_import" : raw.trim();
        if (base.equals("tmp_wlh_import") || !base.matches(".*_\\d+$")) {
            int idx = 1;
            String candidate;
            do {
                candidate = "tmp_wlh_import_" + idx;
                idx++;
            } while (tableExists(schema, candidate));
            return candidate;
        }
        return base;
    }

    private boolean tableExists(String schema, String table) {
        String safeSchema = schema.replace("'", "''");
        String safeTable = table.replace("'", "''");
        String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema='" + safeSchema + "' AND table_name='" + safeTable + "' LIMIT 1";
        try {
            SqlExecResult result = sqlExecutionService.executeSync(sql, 1);
            return result.getRows() != null && !result.getRows().isEmpty();
        } catch (Exception e) {
            OperationLog.log("检测表存在性失败: " + e.getMessage());
            return false;
        }
    }

    private void startImport() {
        if (importing.get()) {
            return;
        }
        if (tempFile == null || !Files.exists(tempFile)) {
            JOptionPane.showMessageDialog(this, "请先粘贴数据", "缺少数据", JOptionPane.WARNING_MESSAGE);
            return;
        }
        importing.set(true);
        progressBar.setIndeterminate(false);
        progressBar.setMinimum(0);
        progressBar.setMaximum(Math.max(1, sampledTotalRows));
        progressText.setText("准备导入...");
        List<ColumnMeta> selected = collectColumnsFromTable();
        if (selected.isEmpty()) {
            JOptionPane.showMessageDialog(this, "请选择至少一列导入", "无列", JOptionPane.WARNING_MESSAGE);
            importing.set(false);
            progressBar.setIndeterminate(false);
            return;
        }
        String schema = Objects.toString(schemaCombo.getSelectedItem(), "public");
        String baseName = ensureImportSuffix(schema, tableNameField.getText());
        String userStrategy = Objects.toString(strategyCombo.getSelectedItem(), "自动适配");
        SwingWorker<Void, String> worker = new SwingWorker<>() {
            private String finalTable;
            private String appliedStrategy;
            @Override
            protected Void doInBackground() throws Exception {
                finalTable = resolveFinalTable(schema, baseName, userStrategy);
                appliedStrategy = determineAppliedStrategy(schema, finalTable, userStrategy, selected);
                SwingUtilities.invokeLater(() -> tableNameField.setText(finalTable));
                OperationLog.log("最终表名: " + finalTable + " 策略: " + appliedStrategy);
                publish("策略: " + appliedStrategy + " | 表: " + finalTable);
                executeSql("BEGIN");
                boolean created = false;
                boolean dropped = false;
                try {
                    if ("删除后重建".equals(appliedStrategy)) {
                        executeSql("DROP TABLE IF EXISTS \"" + schema + "\".\"" + finalTable + "\"");
                        dropped = true;
                        executeSql(buildCreateTable(schema, finalTable, selected));
                        created = true;
                    } else if ("清空插入".equals(appliedStrategy)) {
                        if (!tableExists(schema, finalTable)) {
                            executeSql(buildCreateTable(schema, finalTable, selected));
                            created = true;
                        } else {
                            executeSql("TRUNCATE TABLE \"" + schema + "\".\"" + finalTable + "\"" + " RESTART IDENTITY");
                        }
                    } else if ("直接追加".equals(appliedStrategy)) {
                        if (!tableExists(schema, finalTable)) {
                            publish("目标表不存在，降级为删除后重建");
                            executeSql(buildCreateTable(schema, finalTable, selected));
                            created = true;
                        }
                    } else { // 自动适配推断后的策略
                        if (!tableExists(schema, finalTable)) {
                            executeSql(buildCreateTable(schema, finalTable, selected));
                            created = true;
                        } else {
                            executeSql("TRUNCATE TABLE \"" + schema + "\".\"" + finalTable + "\"" + " RESTART IDENTITY");
                        }
                    }
                    publish("开始插入...");
                    importData(schema, finalTable, selected, this);
                    executeSql("COMMIT");
                } catch (Exception e) {
                    executeSql("ROLLBACK");
                    if (created && !"直接追加".equals(appliedStrategy)) {
                        executeSql("DROP TABLE IF EXISTS \"" + schema + "\".\"" + finalTable + "\"");
                    }
                    throw e;
                }
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                if (!chunks.isEmpty()) {
                    progressText.setText(chunks.get(chunks.size() - 1));
                }
            }

            @Override
            protected void done() {
                importing.set(false);
                progressBar.setIndeterminate(false);
                try {
                    get();
                    progressBar.setValue(100);
                    progressText.setText("导入完成，表名: " + finalTable + " | 策略: " + appliedStrategy);
                    JOptionPane.showMessageDialog(ExcelPasteImportDialog.this, "导入成功，表名 " + finalTable);
                    cleanupTemp();
                } catch (Exception e) {
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    progressText.setText("导入失败: " + cause.getMessage());
                    JOptionPane.showMessageDialog(ExcelPasteImportDialog.this, cause.getMessage(), "导入失败", JOptionPane.ERROR_MESSAGE);
                }
            }
        };
        currentWorker = worker;
        worker.execute();
    }

    private String resolveFinalTable(String schema, String baseName, String userStrategy) {
        if ("自动适配".equals(userStrategy)) {
            return baseName;
        }
        if ("直接追加".equals(userStrategy) || "清空插入".equals(userStrategy)) {
            return baseName;
        }
        if ("删除后重建".equals(userStrategy)) {
            return baseName;
        }
        return baseName;
    }

    private String determineAppliedStrategy(String schema, String table, String userStrategy, List<ColumnMeta> selected) {
        boolean exists = tableExists(schema, table);
        if ("直接追加".equals(userStrategy)) {
            if (!exists) {
                statusLabel.setText("目标表不存在，降级为删除后重建");
                return "删除后重建";
            }
            return "直接追加";
        }
        if ("清空插入".equals(userStrategy)) {
            if (!exists) {
                return "删除后重建";
            }
            return "清空插入";
        }
        if ("删除后重建".equals(userStrategy)) {
            return "删除后重建";
        }
        // 自动适配
        if (!exists) {
            statusLabel.setText("策略: 自动适配→删除后重建");
            return "删除后重建";
        }
        List<TableColumnMeta> metas = fetchTableColumns(schema, table);
        if (metas.isEmpty()) {
            statusLabel.setText("策略: 自动适配→删除后重建 (无法获取列信息)");
            return "删除后重建";
        }
        if (isCompatible(selected, metas)) {
            statusLabel.setText("策略: 自动适配→清空插入");
            return "清空插入";
        }
        statusLabel.setText("策略: 自动适配→删除后重建 (结构不兼容)");
        return "删除后重建";
    }

    private void executeSql(String sql) {
        SqlExecResult result = sqlExecutionService.executeSync(sql, 0);
        if (!result.isSuccess()) {
            DatabaseErrorInfo err = result.getError();
            String message = err != null ? err.getRaw() : result.getMessage();
            throw new SqlExecutionException(message, err);
        }
    }

    private String buildCreateTable(String schema, String table, List<ColumnMeta> selected) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE \"").append(schema).append("\".\"").append(table).append("\" (");
        for (int i = 0; i < selected.size(); i++) {
            ColumnMeta c = selected.get(i);
            if (i > 0) sb.append(", ");
            sb.append("\"").append(c.targetName()).append("\" ").append(normalizeType(c.inferredType()));
        }
        sb.append(")");
        OperationLog.log("建表 SQL: " + sb);
        return sb.toString();
    }

    private void importData(String schema, String table, List<ColumnMeta> selected, SwingWorker<?, ?> worker) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(tempFile, StandardCharsets.UTF_8)) {
            // skip header
            reader.readLine();
            String line;
            int rowNum = 0;
            int batchRowCount = 0;
            StringBuilder sqlBuilder = new StringBuilder();
            String insertHead = buildInsertHead(schema, table, selected);
            while ((line = reader.readLine()) != null) {
                if (Thread.currentThread().isInterrupted() || (worker != null && worker.isCancelled())) {
                    throw new IOException("已取消");
                }
                String[] parts = line.split("\t", -1);
                String tuple = buildTuple(parts, selected);
                if (sqlBuilder.length() == 0) {
                    sqlBuilder.append(insertHead);
                } else {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(tuple);
                batchRowCount++;
                rowNum++;
                if (batchRowCount >= DEFAULT_BATCH_ROWS || sqlBuilder.length() >= MAX_SQL_SIZE) {
                    flushInsert(sqlBuilder.toString(), rowNum - batchRowCount + 1, rowNum);
                    sqlBuilder.setLength(0);
                    batchRowCount = 0;
                }
                if (rowNum % 100 == 0) {
                    int current = rowNum;
                    SwingUtilities.invokeLater(() -> progressBar.setValue(current));
                    progressText.setText("已处理 " + rowNum + " 行");
                }
            }
            if (batchRowCount > 0 && sqlBuilder.length() > 0) {
                flushInsert(sqlBuilder.toString(), rowNum - batchRowCount + 1, rowNum);
            }
        }
    }

    private String buildInsertHead(String schema, String table, List<ColumnMeta> selected) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO \"").append(schema).append("\".\"").append(table).append("\" (");
        for (int i = 0; i < selected.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(selected.get(i).targetName()).append("\"");
        }
        sb.append(") VALUES ");
        return sb.toString();
    }

    private String buildTuple(String[] parts, List<ColumnMeta> selected) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < selected.size(); i++) {
            ColumnMeta c = selected.get(i);
            if (i > 0) sb.append(", ");
            String value = c.index < parts.length ? parts[c.index] : "";
            sb.append(formatValue(value, c.inferredType()));
        }
        sb.append(")");
        return sb.toString();
    }

    private String formatValue(String raw, String type) {
        if (raw == null || raw.isBlank()) {
            return "NULL";
        }
        String sanitized = raw.replace("'", "''");
        String lower = type.toLowerCase(Locale.ROOT);
        if (lower.contains("int") || lower.startsWith("bigint")) {
            return "CAST('" + sanitized + "' AS " + type + ")";
        }
        if (lower.startsWith("numeric")) {
            return "CAST('" + sanitized + "' AS " + type + ")";
        }
        if (lower.startsWith("timestamp") || lower.startsWith("date") || lower.startsWith("boolean") || lower.startsWith("uuid") || lower.startsWith("jsonb")) {
            return "CAST('" + sanitized + "' AS " + type + ")";
        }
        return "'" + sanitized + "'";
    }

    private void flushInsert(String sql, int startRow, int endRow) {
        try {
            executeSql(sql);
        } catch (Exception e) {
            throw new RuntimeException("批次 " + startRow + "-" + endRow + " 失败: " + e.getMessage(), e);
        }
    }

    private List<ColumnMeta> collectColumnsFromTable() {
        List<ColumnMeta> list = new ArrayList<>();
        DefaultTableModel model = (DefaultTableModel) fieldTable.getModel();
        for (int i = 0; i < model.getRowCount(); i++) {
            Boolean enabled = (Boolean) model.getValueAt(i, 0);
            String source = Objects.toString(model.getValueAt(i, 1), "");
            String target = Objects.toString(model.getValueAt(i, 2), source);
            String type = normalizeType(Objects.toString(model.getValueAt(i, 3), "text"));
            if (Boolean.TRUE.equals(enabled)) {
                int originalIndex = i;
                if (i < columns.size()) {
                    originalIndex = columns.get(i).index;
                }
                list.add(new ColumnMeta(source, target, type, originalIndex));
            }
        }
        return list;
    }

    private void cancelCurrentWorker() {
        SwingWorker<?, ?> worker = this.currentWorker;
        if (worker != null && !worker.isDone()) {
            worker.cancel(true);
        }
    }

    private void cleanupTemp() {
        if (tempFile != null) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException ignored) {
            }
            tempFile = null;
        }
    }

    private void resetAll() {
        cancelCurrentWorker();
        cleanupTemp();
        pasteArea.setText("");
        pastePreviewArea.setText("");
        previewTable.setModel(new DefaultTableModel());
        columns = new ArrayList<>();
        lastSample = null;
        sampledTotalRows = 0;
        progressBar.setValue(0);
        progressText.setText("已重置，等待粘贴");
        tableNameField.setText("tmp_wlh_import_1");
        fieldTable.setModel(new DefaultTableModel(new Object[][]{}, new String[]{"导入", "源列", "目标字段名", "目标类型"}));
        typeNormalizedNotified = false;
    }

    private String[] allowedTypes() {
        return new String[]{"boolean", "integer", "bigint", "numeric", "date", "timestamp", "uuid", "jsonb", "varchar", "text"};
    }

    private String normalizeType(String raw) {
        if (raw == null) return "text";
        String lower = raw.trim().toLowerCase(Locale.ROOT);
        if (lower.contains("varchar")) {
            return "varchar";
        }
        if (lower.contains("int8") || lower.contains("bigint")) {
            return "bigint";
        }
        if (lower.contains("int4") || lower.contains("integer")) {
            return "integer";
        }
        if (lower.startsWith("numeric")) {
            return "numeric";
        }
        if (lower.startsWith("timestamp")) {
            return "timestamp";
        }
        if (lower.startsWith("date")) {
            return "date";
        }
        if (lower.startsWith("uuid")) {
            return "uuid";
        }
        if (lower.startsWith("json")) {
            return "jsonb";
        }
        if (lower.startsWith("bool")) {
            return "boolean";
        }
        if (lower.startsWith("text")) {
            return "text";
        }
        return lower.isBlank() ? "text" : lower;
    }

    private void notifyTypeNormalized() {
        if (!typeNormalizedNotified) {
            statusLabel.setText(statusLabel.getText() + " | 已规范化类型");
            typeNormalizedNotified = true;
        }
    }

    private List<TableColumnMeta> fetchTableColumns(String schema, String table) {
        String safeSchema = schema.replace("'", "''");
        String safeTable = table.replace("'", "''");
        String sql = "SELECT column_name, data_type, ordinal_position FROM information_schema.columns WHERE table_schema='" + safeSchema + "' AND table_name='" + safeTable + "' ORDER BY ordinal_position";
        try {
            SqlExecResult result = sqlExecutionService.executeSync(sql, 0);
            List<TableColumnMeta> list = new ArrayList<>();
            if (result.getRows() != null) {
                for (List<?> row : result.getRows()) {
                    String name = Objects.toString(row.get(0), "");
                    String type = Objects.toString(row.get(1), "text");
                    list.add(new TableColumnMeta(name, normalizeType(type), list.size()));
                }
            }
            return list;
        } catch (Exception e) {
            OperationLog.log("读取列元数据失败: " + e.getMessage());
            return List.of();
        }
    }

    private boolean isCompatible(List<ColumnMeta> target, List<TableColumnMeta> existing) {
        if (target.isEmpty()) {
            return false;
        }
        LinkedHashMap<String, String> targetMap = new LinkedHashMap<>();
        for (ColumnMeta meta : target) {
            targetMap.put(meta.targetName().toLowerCase(Locale.ROOT), normalizeType(meta.inferredType()));
        }
        for (String name : targetMap.keySet()) {
            TableColumnMeta found = null;
            for (TableColumnMeta c : existing) {
                if (c.name().equalsIgnoreCase(name)) {
                    found = c;
                    break;
                }
            }
            if (found == null) {
                return false;
            }
            if (!isTypeCastable(targetMap.get(name), found.type())) {
                return false;
            }
        }
        return true;
    }

    private boolean isTypeCastable(String targetType, String existingType) {
        String t = normalizeType(targetType);
        String e = normalizeType(existingType);
        if (t.equals(e)) {
            return true;
        }
        Set<String> numericGroup = Set.of("integer", "bigint", "numeric");
        if (numericGroup.contains(t) && numericGroup.contains(e)) {
            return true;
        }
        Set<String> textGroup = Set.of("varchar", "text");
        if (textGroup.contains(t) && textGroup.contains(e)) {
            return true;
        }
        if ((t.equals("date") && e.equals("timestamp")) || (t.equals("timestamp") && e.equals("date"))) {
            return true;
        }
        return false;
    }

    private record ColumnMeta(String sourceName, String targetName, String inferredType, int index) {
        ColumnMeta(String sourceName, String targetName, String inferredType) {
            this(sourceName, targetName, inferredType, -1);
        }
    }

    private record SampleResult(List<List<String>> preview, List<ColumnMeta> columns, int totalRows, int maxCols) {
    }

    private record TableColumnMeta(String name, String type, int index) {
    }

    private static class ColumnStats {
        private int nonEmpty = 0;
        private boolean boolCandidate = true;
        private boolean intCandidate = true;
        private boolean bigintCandidate = true;
        private boolean numericCandidate = true;
        private boolean dateCandidate = true;
        private boolean tsCandidate = true;
        private boolean uuidCandidate = true;
        private boolean jsonCandidate = true;
        private int maxLen = 0;
        private int jsonOk = 0;

        void accept(String raw) {
            if (raw == null || raw.isBlank()) {
                return;
            }
            String v = raw.trim();
            nonEmpty++;
            maxLen = Math.max(maxLen, v.length());
            if (boolCandidate) {
                boolCandidate = isBoolean(v);
            }
            if (intCandidate) {
                intCandidate = isInt(v);
            }
            if (bigintCandidate) {
                bigintCandidate = isBigInt(v);
            }
            if (numericCandidate) {
                numericCandidate = isNumeric(v);
            }
            if (dateCandidate) {
                dateCandidate = isDate(v);
            }
            if (tsCandidate) {
                tsCandidate = isTimestamp(v);
            }
            if (uuidCandidate) {
                uuidCandidate = isUuid(v);
            }
            if (jsonCandidate) {
                boolean ok = isJson(v);
                jsonCandidate = ok || jsonOk * 100 / Math.max(1, nonEmpty) >= 95;
                if (ok) jsonOk++;
            }
        }

        String inferType() {
            if (nonEmpty == 0) {
                return "text";
            }
            double ratioBool = boolCandidate ? 1.0 : 0.0;
            double ratioInt = intCandidate ? 1.0 : 0.0;
            double ratioBigInt = bigintCandidate ? 1.0 : 0.0;
            double ratioDate = dateCandidate ? 1.0 : 0.0;
            double ratioTs = tsCandidate ? 1.0 : 0.0;
            double ratioUuid = uuidCandidate ? 1.0 : 0.0;
            double ratioJson = nonEmpty == 0 ? 0 : (jsonOk * 1.0 / nonEmpty);
            if (ratioBool >= 0.98) return "boolean";
            if (ratioInt >= 0.98) return "integer";
            if (ratioBigInt >= 0.98) return "bigint";
            if (numericCandidate) return "numeric";
            if (ratioDate >= 0.98) return "date";
            if (ratioTs >= 0.98) return "timestamp";
            if (ratioUuid >= 0.98) return "uuid";
            if (ratioJson >= 0.95) return "jsonb";
            return fallbackVarchar();
        }

        private String fallbackVarchar() {
            if (maxLen <= 255) {
                return "varchar";
            }
            return "text";
        }

        private boolean isBoolean(String v) {
            String lower = v.toLowerCase(Locale.ROOT);
            return lower.matches("true|false|t|f|1|0|是|否");
        }

        private boolean isInt(String v) {
            try {
                long val = Long.parseLong(v.trim());
                return val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        private boolean isBigInt(String v) {
            try {
                Long.parseLong(v.trim());
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        private boolean isNumeric(String v) {
            return v.matches("[+-]?\\d+(\\.\\d+)?");
        }

        private boolean isDate(String v) {
            String[] patterns = {"yyyy-MM-dd", "yyyy/MM/dd", "yyyy.MM.dd"};
            for (String p : patterns) {
                try {
                    LocalDate.parse(v, DateTimeFormatter.ofPattern(p));
                    return true;
                } catch (DateTimeParseException ignored) {
                }
            }
            return false;
        }

        private boolean isTimestamp(String v) {
            try {
                LocalDateTime.parse(v, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]"));
                return true;
            } catch (DateTimeParseException ignored) {
            }
            try {
                OffsetDateTime.parse(v);
                return true;
            } catch (DateTimeParseException ignored) {
            }
            return false;
        }

        private boolean isUuid(String v) {
            try {
                UUID.fromString(v);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        private boolean isJson(String v) {
            String trimmed = v.trim();
            if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) {
                return false;
            }
            // 粗略校验
            int len = trimmed.length();
            return len >= 2;
        }
    }

    private static class ClipboardPayload {
        private final String html;
        private final String tsv;

        private ClipboardPayload(String html, String tsv) {
            this.html = html;
            this.tsv = tsv;
        }

        static ClipboardPayload fromHtml(String html) {
            return new ClipboardPayload(html, null);
        }

        static ClipboardPayload fromTsv(String text) {
            return new ClipboardPayload(null, text);
        }

        void writeAsTsv(Writer writer) throws IOException {
            if (tsv != null) {
                writer.write(tsv);
                return;
            }
            parseHtmlToTsv(html, writer);
        }

        private void parseHtmlToTsv(String htmlContent, Writer writer) throws IOException {
            ParserDelegator delegator = new ParserDelegator();
            delegator.parse(new StringReader(htmlContent), new HTMLEditorKit.ParserCallback() {
                private List<String> currentRow;
                private StringBuilder cell;
                private boolean inCell;
                private boolean firstRowWritten = false;

                @Override
                public void handleStartTag(HTML.Tag t, javax.swing.text.MutableAttributeSet a, int pos) {
                    if (t == HTML.Tag.TR) {
                        currentRow = new ArrayList<>();
                    } else if (t == HTML.Tag.TD || t == HTML.Tag.TH) {
                        inCell = true;
                        cell = new StringBuilder();
                    }
                }

                @Override
                public void handleText(char[] data, int pos) {
                    if (inCell && cell != null) {
                        cell.append(data);
                    }
                }

                @Override
                public void handleEndTag(HTML.Tag t, int pos) {
                    if (t == HTML.Tag.TD || t == HTML.Tag.TH) {
                        if (currentRow != null) {
                            currentRow.add(cell == null ? "" : cell.toString().trim());
                        }
                        inCell = false;
                    } else if (t == HTML.Tag.TR) {
                        if (currentRow != null && !currentRow.isEmpty()) {
                            try {
                                if (firstRowWritten) {
                                    writer.write('\n');
                                }
                                for (int i = 0; i < currentRow.size(); i++) {
                                    if (i > 0) writer.write('\t');
                                    writer.write(currentRow.get(i));
                                }
                                firstRowWritten = true;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        currentRow = null;
                    }
                }
            }, true);
        }
    }
}
