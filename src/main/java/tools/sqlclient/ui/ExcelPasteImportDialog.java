package tools.sqlclient.ui;

import tools.sqlclient.exec.DatabaseErrorInfo;
import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionException;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.util.OperationLog;

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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

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

    private static final Set<String> RESERVED = Set.of(
            "user", "select", "table", "from", "where", "group", "order", "limit",
            "offset", "join", "insert", "update", "delete"
    );

    private final SqlExecutionService sqlExecutionService;

    private final JTextArea pasteArea = new JTextArea();
    private final JTable previewTable = new JTable();
    private final JTable fieldTable = new JTable();
    private final JLabel statusLabel = new JLabel("请粘贴包含表头的数据");
    private final JLabel progressText = new JLabel("等待粘贴");
    private final JProgressBar progressBar = new JProgressBar();
    private final JTextField tableNameField = new JTextField();
    private final javax.swing.JComboBox<String> schemaCombo = new javax.swing.JComboBox<>();
    private final JToggleButton fieldToggle = new JToggleButton("字段设置");
    private final JPanel fieldPanel = new JPanel(new BorderLayout());
    private final AtomicBoolean parsing = new AtomicBoolean(false);
    private final AtomicBoolean importing = new AtomicBoolean(false);

    private Path tempFile;
    private List<ColumnMeta> columns = new ArrayList<>();
    private SwingWorker<?, ?> currentWorker;
    private int sampledTotalRows;

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

        statusLabel.setFont(statusLabel.getFont().deriveFont(Font.PLAIN, 12f));
        statusLabel.setForeground(java.awt.Color.DARK_GRAY);

        top.add(schemaLabel);
        top.add(schemaCombo);
        top.add(tableLabel);
        top.add(tableNameField);
        top.add(statusLabel);
        add(top, BorderLayout.NORTH);

        JSplitPane centerSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        centerSplit.setResizeWeight(0.45);

        pasteArea.setLineWrap(true);
        pasteArea.setWrapStyleWord(false);
        pasteArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        pasteArea.setBorder(BorderFactory.createTitledBorder("粘贴区域 (Ctrl+V)"));
        JScrollPane leftScroll = new JScrollPane(pasteArea);
        centerSplit.setLeftComponent(leftScroll);

        previewTable.setModel(new DefaultTableModel());
        previewTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        JScrollPane previewScroll = new JScrollPane(previewTable);
        previewScroll.setBorder(BorderFactory.createTitledBorder("预览 (仅前" + PREVIEW_ROWS + "行)"));
        centerSplit.setRightComponent(previewScroll);

        add(centerSplit, BorderLayout.CENTER);

        // 字段配置区
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

        JScrollPane fieldScroll = new JScrollPane(fieldTable);
        fieldPanel.add(fieldScroll, BorderLayout.CENTER);

        JPanel fieldHeader = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 4));
        fieldToggle.addActionListener(e -> {
            fieldPanel.setVisible(fieldToggle.isSelected());
            fieldPanel.revalidate();
            fieldPanel.repaint();
        });

        JButton resetBtn = new JButton(new javax.swing.AbstractAction("重置推断") {
            @Override
            public void actionPerformed(ActionEvent e) {
                applyColumns(columns);
            }
        });

        fieldHeader.add(fieldToggle);
        fieldHeader.add(resetBtn);

        JPanel fieldWrapper = new JPanel(new BorderLayout());
        fieldWrapper.add(fieldHeader, BorderLayout.NORTH);
        fieldWrapper.add(fieldPanel, BorderLayout.CENTER);

        // 底部区
        JPanel bottom = new JPanel(new BorderLayout());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 8));
        JButton importBtn = new JButton("导入");
        JButton cancelBtn = new JButton("取消");
        buttons.add(importBtn);
        buttons.add(cancelBtn);

        progressBar.setStringPainted(true);
        progressText.setHorizontalAlignment(SwingConstants.LEFT);

        JPanel progressPanel = new JPanel(new BorderLayout());
        progressPanel.setBorder(new EmptyBorder(0, 8, 4, 8));
        progressPanel.add(progressBar, BorderLayout.CENTER);
        progressPanel.add(progressText, BorderLayout.SOUTH);

        bottom.add(progressPanel, BorderLayout.CENTER);
        bottom.add(buttons, BorderLayout.EAST);

        // 关键：避免 BorderLayout.SOUTH / PAGE_END 覆盖
        JPanel south = new JPanel(new BorderLayout());
        south.add(fieldWrapper, BorderLayout.CENTER);
        south.add(bottom, BorderLayout.SOUTH);
        add(south, BorderLayout.SOUTH);

        cancelBtn.addActionListener(e -> {
            cancelCurrentWorker();
            SwingUtilities.invokeLater(() -> progressText.setText("已取消"));
        });
        importBtn.addActionListener(e -> startImport());
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
        SwingUtilities.invokeLater(() -> progressText.setText("读取剪贴板..."));

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
                    SwingUtilities.invokeLater(() -> progressText.setText("已写入临时文件，开始采样..."));

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
                    SwingUtilities.invokeLater(() -> progressText.setText("读取失败: " + error));
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
                Object data = clipboard.getData(DataFlavor.allHtmlFlavor);
                String html = data instanceof String ? (String) data : null;

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
            SwingUtilities.invokeLater(() -> progressText.setText("暂无临时文件"));
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
                    SwingUtilities.invokeLater(() -> progressText.setText("解析失败: " + e.getMessage()));
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
                    preview.add(Arrays.asList(parts)); // 原始表头行，仅用于预览占位
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

                if (preview.size() < PREVIEW_ROWS + 1) {
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
        this.sampledTotalRows = result.totalRows();

        statusLabel.setText("Rows=" + result.totalRows() + " Cols=" + result.maxCols());
        progressText.setText("已完成采样，推断字段类型");

        applyPreview(result.preview());
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
        for (int i = 1; i < data.size(); i++) { // 跳过 header 行
            List<String> row = data.get(i);
            Object[] arr = new Object[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                arr[c] = c < row.size() ? row.get(c) : "";
            }
            model.addRow(arr);
        }
        previewTable.setModel(model);
    }

    private void applyColumns(List<ColumnMeta> cols) {
        DefaultTableModel model = (DefaultTableModel) fieldTable.getModel();
        model.setRowCount(0);
        for (ColumnMeta c : cols) {
            model.addRow(new Object[]{Boolean.TRUE, c.sourceName(), c.targetName(), c.inferredType()});
        }
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
            String base = (h == null || h.isBlank()) ? ("col_" + index) : h;
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
        return RESERVED.contains(name);
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
        if (base == null || base.isBlank()) {
            base = "tmp_wlh_import_1";
        }
        String schema = Objects.toString(schemaCombo.getSelectedItem(), "public");
        String finalName = resolveAvailableName(schema, base.trim());
        tableNameField.setText(finalName);
        statusLabel.setText(statusLabel.getText() + " | 最终表名: " + finalName);
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
        String baseName = tableNameField.getText().trim();

        SwingWorker<Void, String> worker = new SwingWorker<>() {
            private String finalTable;

            @Override
            protected Void doInBackground() throws Exception {
                finalTable = resolveAvailableName(schema, baseName);
                OperationLog.log("最终表名: " + finalTable);

                publish("建表: " + finalTable);
                executeSql("BEGIN");

                boolean created = false;
                try {
                    executeSql(buildCreateTable(schema, finalTable, selected));
                    created = true;

                    publish("开始插入...");
                    importData(schema, finalTable, selected, this::isCancelled);

                    executeSql("COMMIT");
                } catch (Exception e) {
                    try {
                        executeSql("ROLLBACK");
                    } catch (Exception ignore) {
                    }
                    if (created) {
                        try {
                            executeSql("DROP TABLE IF EXISTS \"" + schema + "\".\"" + finalTable + "\"");
                        } catch (Exception ignore) {
                        }
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
                    progressBar.setValue(progressBar.getMaximum());
                    progressText.setText("导入完成，表名: " + finalTable);
                    JOptionPane.showMessageDialog(ExcelPasteImportDialog.this, "导入成功，表名 " + finalTable);
                } catch (Exception e) {
                    Throwable cause = (e.getCause() == null) ? e : e.getCause();
                    progressText.setText("导入失败: " + cause.getMessage());
                    JOptionPane.showMessageDialog(ExcelPasteImportDialog.this, cause.getMessage(), "导入失败", JOptionPane.ERROR_MESSAGE);
                } finally {
                    cleanupTemp();
                }
            }
        };

        currentWorker = worker;
        worker.execute();
    }

    private void executeSql(String sql) {
        SqlExecResult result = sqlExecutionService.executeSync(sql, 0);
        if (!result.isSuccess()) {
            DatabaseErrorInfo err = result.getError();
            String message = (err != null) ? err.getRaw() : result.getMessage();
            throw new SqlExecutionException(message, err);
        }
    }

    private String buildCreateTable(String schema, String table, List<ColumnMeta> selected) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE \"").append(schema).append("\".\"").append(table).append("\" (");
        for (int i = 0; i < selected.size(); i++) {
            ColumnMeta c = selected.get(i);
            if (i > 0) sb.append(", ");
            sb.append("\"").append(c.targetName()).append("\" ").append(c.inferredType());
        }
        sb.append(")");
        OperationLog.log("建表 SQL: " + sb);
        return sb.toString();
    }

    /**
     * 修复点：不再使用 SwingWorker.this（importData 不在 SwingWorker 的封闭作用域）。
     * 通过 BooleanSupplier 注入取消状态检查。
     */
    private void importData(String schema, String table, List<ColumnMeta> selected, BooleanSupplier cancelled) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(tempFile, StandardCharsets.UTF_8)) {
            reader.readLine(); // skip header
            String line;

            int rowNum = 0;
            int batchRowCount = 0;

            StringBuilder sqlBuilder = new StringBuilder();
            String insertHead = buildInsertHead(schema, table, selected);

            while ((line = reader.readLine()) != null) {
                if (Thread.currentThread().isInterrupted() || (cancelled != null && cancelled.getAsBoolean())) {
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
                    SwingUtilities.invokeLater(() -> {
                        progressBar.setValue(current);
                        progressText.setText("已处理 " + current + " 行");
                    });
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
            String value = (c.index < parts.length) ? parts[c.index] : "";
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
        if (lower.startsWith("timestamp") || lower.startsWith("date") || lower.startsWith("boolean")
                || lower.startsWith("uuid") || lower.startsWith("jsonb")) {
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
            String type = Objects.toString(model.getValueAt(i, 3), "text");
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

    private record ColumnMeta(String sourceName, String targetName, String inferredType, int index) {
        ColumnMeta(String sourceName, String targetName, String inferredType) {
            this(sourceName, targetName, inferredType, -1);
        }
    }

    private record SampleResult(List<List<String>> preview, List<ColumnMeta> columns, int totalRows, int maxCols) {
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
        private int maxPrecision = 0;
        private int maxScale = 0;
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
                if (numericCandidate) {
                    updatePrecision(v);
                }
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
                if (ok) jsonOk++;
                jsonCandidate = ok || jsonOk * 100 / Math.max(1, nonEmpty) >= 95;
            }
        }

        private void updatePrecision(String v) {
            String digits = v.replaceFirst("^[+-]", "");
            int scale;
            int precision;
            int dot = digits.indexOf('.');
            if (dot >= 0) {
                precision = digits.replace(".", "").replaceFirst("^0+", "").length();
                scale = digits.length() - dot - 1;
            } else {
                precision = digits.replaceFirst("^0+", "").length();
                scale = 0;
            }
            maxPrecision = Math.max(maxPrecision, precision);
            maxScale = Math.max(maxScale, scale);
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
            double ratioJson = (nonEmpty == 0) ? 0.0 : (jsonOk * 1.0 / nonEmpty);

            if (ratioBool >= 0.98) return "boolean";
            if (ratioInt >= 0.98) return "integer";
            if (ratioBigInt >= 0.98) return "bigint";

            if (numericCandidate) {
                int p = Math.min(38, Math.max(1, maxPrecision));
                int s = Math.min(38, Math.max(0, maxScale));
                return "numeric(" + p + "," + s + ")";
            }

            if (ratioDate >= 0.98) return "date";
            if (ratioTs >= 0.98) return "timestamp";
            if (ratioUuid >= 0.98) return "uuid";
            if (ratioJson >= 0.95) return "jsonb";

            return fallbackVarchar();
        }

        private String fallbackVarchar() {
            if (maxLen <= 255) {
                return "varchar(255)";
            }
            if (maxLen <= 500) {
                return "varchar(500)";
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
                // 常见格式：2025-01-01 12:34:56 或 2025-01-01 12:34:56.123
                DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");
                LocalDateTime.parse(v, f);
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
            return trimmed.length() >= 2;
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
