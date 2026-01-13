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
import javax.swing.table.TableCellRenderer;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.awt.BorderLayout;
import java.awt.Color;
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
import java.util.Collections;
import java.util.EnumSet;
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

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

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
    private final JTable fieldTable = new JTable() {
        @Override
        public java.awt.Component prepareRenderer(TableCellRenderer renderer, int row, int column) {
            java.awt.Component c = super.prepareRenderer(renderer, row, column);
            if (!isRowSelected(row)) {
                if (invalidRows.contains(row)) {
                    c.setBackground(new Color(255, 235, 235));
                } else {
                    c.setBackground(Color.WHITE);
                }
            }
            return c;
        }
    };
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
    private final Set<Integer> invalidRows = new HashSet<>();
    private final JButton importBtn = new JButton("导入");
    private Path tempFile;
    private List<ColumnMeta> columns = new ArrayList<>();
    private SwingWorker<?, ?> currentWorker;
    private SwingWorker<?, ?> fullInferenceWorker;
    private int sampledTotalRows;
    private SampleResult lastSample;
    private boolean typeNormalizedNotified;
    private boolean fullInferenceFinished;

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

        fieldToggle.setSelected(true);
        fieldPanel.setVisible(true);
        fieldPanel.setBorder(BorderFactory.createTitledBorder("字段设置（可修改目标名/类型/导入勾选）"));
        fieldTable.setModel(new DefaultTableModel(new Object[][]{}, new String[]{"导入", "源列", "目标字段名", "目标类型", "来源"}) {
            @Override
            public Class<?> getColumnClass(int columnIndex) {
                return switch (columnIndex) {
                    case 0 -> Boolean.class;
                    default -> String.class;
                };
            }

            @Override
            public boolean isCellEditable(int row, int column) {
                return column != 1 && column != 4;
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
        JButton moveUpBtn = new JButton("上移");
        moveUpBtn.addActionListener(e -> moveSelectedRow(-1));
        JButton moveDownBtn = new JButton("下移");
        moveDownBtn.addActionListener(e -> moveSelectedRow(1));
        JButton resetInferenceBtn = new JButton(new javax.swing.AbstractAction("重置推断") {
            @Override
            public void actionPerformed(ActionEvent e) {
                applyColumns(columns);
            }
        });
        fieldHeader.add(fieldToggle);
        fieldHeader.add(moveUpBtn);
        fieldHeader.add(moveDownBtn);
        fieldHeader.add(resetInferenceBtn);
        JPanel fieldWrapper = new JPanel(new BorderLayout());
        fieldWrapper.add(fieldHeader, BorderLayout.NORTH);
        fieldWrapper.add(fieldPanel, BorderLayout.CENTER);
        add(fieldWrapper, BorderLayout.SOUTH);

        JPanel bottom = new JPanel(new BorderLayout());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 8));
        javax.swing.JButton cancelBtn = new javax.swing.JButton("取消");
        javax.swing.JButton resetBtn = new javax.swing.JButton("重置");
        importBtn.setEnabled(false);
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
            List<ColumnMeta> metas = buildColumns(header, stats, TypeSource.SAMPLE);
            return new SampleResult(preview, metas, totalRows, maxCols);
        }
    }

    private void applySample(SampleResult result) {
        this.columns = result.columns();
        this.lastSample = result;
        this.sampledTotalRows = result.totalRows();
        this.typeNormalizedNotified = false;
        this.fullInferenceFinished = false;
        statusLabel.setText("Rows=" + result.totalRows() + " Cols=" + result.maxCols() + " | 类型推断中...");
        progressText.setText("已完成采样，启动全量推断...");
        applyPreview(result.preview());
        applyPastePreview(result.preview());
        applyColumns(result.columns());
        suggestTableName();
        progressBar.setIndeterminate(true);
        progressBar.setMinimum(0);
        progressBar.setMaximum(Math.max(1, sampledTotalRows));
        progressBar.setValue(0);
        startFullInference();
    }

    private void startFullInference() {
        if (tempFile == null || !Files.exists(tempFile)) {
            return;
        }
        if (fullInferenceWorker != null && !fullInferenceWorker.isDone()) {
            fullInferenceWorker.cancel(true);
        }
        progressText.setText("类型推断中（全量）...");
        SwingWorker<List<ColumnMeta>, Void> worker = new SwingWorker<>() {
            @Override
            protected List<ColumnMeta> doInBackground() throws Exception {
                return inferAllRows(tempFile);
            }

            @Override
            protected void done() {
                progressBar.setIndeterminate(false);
                try {
                    List<ColumnMeta> inferred = get();
                    applyFullInference(inferred);
                    progressBar.setValue(progressBar.getMaximum());
                } catch (Exception e) {
                    progressText.setText("全量推断失败: " + e.getMessage());
                    OperationLog.log("全量推断失败: " + e.getMessage());
                }
            }
        };
        fullInferenceWorker = worker;
        currentWorker = worker;
        worker.execute();
    }

    private List<ColumnMeta> inferAllRows(Path file) throws IOException {
        List<ColumnStats> stats = new ArrayList<>();
        List<String> header = null;
        Set<String> usedNames = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t", -1);
                if (header == null) {
                    header = normalizeHeader(parts);
                    usedNames.addAll(header);
                    stats = initStats(header.size());
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
                for (int i = 0; i < header.size(); i++) {
                    String v = i < parts.length ? parts[i] : "";
                    stats.get(i).accept(v);
                }
            }
        }
        if (header == null) {
            throw new IOException("数据为空");
        }
        return buildColumns(header, stats, TypeSource.FULL);
    }

    private void applyFullInference(List<ColumnMeta> inferred) {
        List<ColumnMeta> merged = new ArrayList<>();
        int size = Math.max(columns.size(), inferred.size());
        for (int i = 0; i < size; i++) {
            ColumnMeta existing = i < columns.size() ? columns.get(i) : null;
            ColumnMeta newest = i < inferred.size() ? inferred.get(i) : null;
            if (existing == null && newest != null) {
                merged.add(newest);
                continue;
            }
            if (existing == null) {
                continue;
            }
            String targetName = existing.nameOverridden() ? existing.targetName() : Objects.requireNonNullElse(newest, existing).targetName();
            boolean nameOverridden = existing.nameOverridden();
            boolean typeOverridden = existing.typeOverridden();
            boolean enabled = existing.enabled();
            String type = typeOverridden || newest == null ? existing.inferredType() : newest.inferredType();
            TypeSource source = typeOverridden ? TypeSource.USER : newest == null ? existing.typeSource() : newest.typeSource();
            merged.add(new ColumnMeta(existing.sourceName(), targetName, type, existing.index(), nameOverridden, typeOverridden, source, enabled));
        }
        if (merged.isEmpty()) {
            merged = inferred;
        }
        applyColumns(merged);
        if (lastSample != null) {
            applyPreview(lastSample.preview());
            applyPastePreview(lastSample.preview());
        }
        fullInferenceFinished = true;
        statusLabel.setText("Rows=" + sampledTotalRows + " Cols=" + merged.size() + " | 类型推断完成（全量）");
        progressText.setText("类型推断完成（全量）");
    }

    private void applyPreview(List<List<String>> data) {
        if (data.isEmpty()) {
            previewTable.setModel(new DefaultTableModel());
            return;
        }
        List<String> header = new ArrayList<>();
        for (ColumnMeta col : columns) {
            header.add(col.sourceName() + " (" + col.inferredType() + "/" + statusText(col) + ")");
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
        this.columns = new ArrayList<>(cols);
        DefaultTableModel model = new DefaultTableModel(new Object[][]{}, new String[]{"导入", "源列", "目标字段名", "目标类型", "来源"}) {
            @Override
            public Class<?> getColumnClass(int columnIndex) {
                return switch (columnIndex) {
                    case 0 -> Boolean.class;
                    default -> String.class;
                };
            }

            @Override
            public boolean isCellEditable(int row, int column) {
                return column != 1 && column != 4;
            }
        };
        for (ColumnMeta c : this.columns) {
            model.addRow(new Object[]{c.enabled(), c.sourceName(), c.targetName(), normalizeType(c.inferredType()), statusText(c)});
        }
        if (!this.columns.isEmpty()) {
            fieldToggle.setSelected(true);
            fieldPanel.setVisible(true);
        }
        model.addTableModelListener(e -> {
            if (e.getType() == javax.swing.event.TableModelEvent.UPDATE) {
                int row = e.getFirstRow();
                if (row < 0 || row >= columns.size()) {
                    return;
                }
                ColumnMeta existing = columns.get(row);
                Boolean enabled = Boolean.TRUE.equals(model.getValueAt(row, 0));
                String targetName = Objects.toString(model.getValueAt(row, 2), existing.targetName());
                String rawType = Objects.toString(model.getValueAt(row, 3), existing.inferredType());
                String normalized = normalizeType(rawType);
                if (!normalized.equals(rawType)) {
                    model.setValueAt(normalized, row, 3);
                    notifyTypeNormalized();
                }
                boolean nameChanged = !Objects.equals(existing.targetName(), targetName);
                boolean typeChanged = !Objects.equals(normalizeType(existing.inferredType()), normalized);
                ColumnMeta updated = existing.withEnabled(enabled);
                if (nameChanged) {
                    updated = updated.withTargetName(targetName);
                }
                if (typeChanged) {
                    updated = updated.withType(normalized, TypeSource.USER, true);
                }
                columns.set(row, updated);
                model.setValueAt(statusText(updated), row, 4);
                validateFieldTable();
            }
        });
        fieldTable.setModel(model);
        TableColumn importCol = fieldTable.getColumnModel().getColumn(0);
        importCol.setCellEditor(new DefaultCellEditor(new JCheckBox()));
        TableColumn typeCol = fieldTable.getColumnModel().getColumn(3);
        typeCol.setCellEditor(new DefaultCellEditor(new javax.swing.JComboBox<>(allowedTypes())));
        validateFieldTable();
    }

    private void moveSelectedRow(int delta) {
        int row = fieldTable.getSelectedRow();
        if (row < 0) {
            return;
        }
        int target = row + delta;
        if (target < 0 || target >= columns.size()) {
            return;
        }
        if (fieldTable.isEditing()) {
            fieldTable.getCellEditor().stopCellEditing();
        }
        Collections.swap(columns, row, target);
        applyColumns(columns);
        if (fieldTable.getRowCount() > target) {
            fieldTable.setRowSelectionInterval(target, target);
            fieldTable.scrollRectToVisible(fieldTable.getCellRect(target, 0, true));
        }
    }

    private String statusText(ColumnMeta c) {
        if (c.nameOverridden() || c.typeOverridden()) {
            return TypeSource.USER.label();
        }
        return c.typeSource().label();
    }

    private void validateFieldTable() {
        invalidRows.clear();
        Set<String> names = new HashSet<>();
        Set<String> duplicates = new HashSet<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta c = columns.get(i);
            if (!c.enabled()) {
                continue;
            }
            String target = Objects.toString(c.targetName(), "").trim();
            if (target.isEmpty() || !isValidIdentifier(target)) {
                invalidRows.add(i);
                continue;
            }
            String lowered = target.toLowerCase(Locale.ROOT);
            if (!names.add(lowered)) {
                duplicates.add(lowered);
            }
        }
        if (!duplicates.isEmpty()) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnMeta c = columns.get(i);
                if (c.enabled() && duplicates.contains(c.targetName().toLowerCase(Locale.ROOT))) {
                    invalidRows.add(i);
                }
            }
        }
        fieldTable.repaint();
        boolean hasEnabled = columns.stream().anyMatch(ColumnMeta::enabled);
        boolean valid = invalidRows.isEmpty() && hasEnabled;
        importBtn.setEnabled(valid);
        if (!valid) {
            statusLabel.setText("字段校验未通过（空/非法/重复）");
        } else if (fullInferenceFinished) {
            statusLabel.setText("Rows=" + sampledTotalRows + " Cols=" + columns.size() + " | 类型推断完成（全量）");
        }
    }

    private boolean isValidIdentifier(String name) {
        return name != null && name.matches("[a-zA-Z_][a-zA-Z0-9_]*");
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

    private List<ColumnMeta> buildColumns(List<String> header, List<ColumnStats> stats, TypeSource typeSource) {
        List<ColumnMeta> metas = new ArrayList<>();
        for (int i = 0; i < header.size(); i++) {
            ColumnStats s = stats.get(i);
            String type = s.inferType();
            metas.add(ColumnMeta.auto(header.get(i), header.get(i), type, i, typeSource));
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
        if (!importBtn.isEnabled()) {
            JOptionPane.showMessageDialog(this, "字段校验未通过，请修正目标字段名/类型", "校验失败", JOptionPane.WARNING_MESSAGE);
            importing.set(false);
            return;
        }
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
        for (ColumnMeta column : columns) {
            if (column.enabled()) {
                list.add(column);
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
        fieldTable.setModel(new DefaultTableModel(new Object[][]{}, new String[]{"导入", "源列", "目标字段名", "目标类型", "来源"}));
        typeNormalizedNotified = false;
        invalidRows.clear();
        importBtn.setEnabled(false);
        fullInferenceFinished = false;
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

    private record ColumnMeta(String sourceName, String targetName, String inferredType, int index, boolean nameOverridden,
                              boolean typeOverridden, TypeSource typeSource, boolean enabled) {
        ColumnMeta(String sourceName, String targetName, String inferredType, int index) {
            this(sourceName, targetName, inferredType, index, false, false, TypeSource.SAMPLE, true);
        }

        static ColumnMeta auto(String sourceName, String targetName, String inferredType, int index, TypeSource source) {
            return new ColumnMeta(sourceName, targetName, inferredType, index, false, false, source, true);
        }

        ColumnMeta withTargetName(String newName) {
            return new ColumnMeta(sourceName, newName, inferredType, index, true, typeOverridden, typeSource, enabled);
        }

        ColumnMeta withType(String newType, TypeSource source, boolean overridden) {
            return new ColumnMeta(sourceName, targetName, newType, index, nameOverridden, overridden, source, enabled);
        }

        ColumnMeta withEnabled(boolean enabled) {
            return new ColumnMeta(sourceName, targetName, inferredType, index, nameOverridden, typeOverridden, typeSource,
                enabled);
        }
    }

    private enum TypeSource {
        SAMPLE("auto(采样)"),
        FULL("auto(全量)"),
        USER("user");

        private final String label;

        TypeSource(String label) {
            this.label = label;
        }

        String label() {
            return label;
        }
    }

    private record SampleResult(List<List<String>> preview, List<ColumnMeta> columns, int totalRows, int maxCols) {
    }

    private record TableColumnMeta(String name, String type, int index) {
    }

    private static class ColumnStats {
        private final Set<DataCandidate> candidates = EnumSet.of(
            DataCandidate.BOOLEAN, DataCandidate.INTEGER, DataCandidate.BIGINT, DataCandidate.NUMERIC,
            DataCandidate.DATE, DataCandidate.TIMESTAMP, DataCandidate.UUID, DataCandidate.JSONB,
            DataCandidate.VARCHAR, DataCandidate.TEXT);
        private int nonEmpty = 0;
        private int maxLen = 0;
        private int jsonOk = 0;

        void accept(String raw) {
            if (raw == null || raw.isBlank()) {
                return;
            }
            String v = raw.trim();
            nonEmpty++;
            maxLen = Math.max(maxLen, v.length());
            if (candidates.contains(DataCandidate.BOOLEAN) && !isBoolean(v)) {
                candidates.remove(DataCandidate.BOOLEAN);
            }
            if (candidates.contains(DataCandidate.INTEGER) && !isInt(v)) {
                candidates.remove(DataCandidate.INTEGER);
            }
            if (candidates.contains(DataCandidate.BIGINT) && !isBigInt(v)) {
                candidates.remove(DataCandidate.BIGINT);
            }
            if (candidates.contains(DataCandidate.NUMERIC) && !isNumeric(v)) {
                candidates.remove(DataCandidate.NUMERIC);
            }
            if (candidates.contains(DataCandidate.DATE) && !isDate(v)) {
                candidates.remove(DataCandidate.DATE);
            }
            if (candidates.contains(DataCandidate.TIMESTAMP) && !isTimestamp(v)) {
                candidates.remove(DataCandidate.TIMESTAMP);
            }
            if (candidates.contains(DataCandidate.UUID) && !isUuid(v)) {
                candidates.remove(DataCandidate.UUID);
            }
            if (candidates.contains(DataCandidate.JSONB)) {
                if (looksLikeJson(v) && isJson(v)) {
                    jsonOk++;
                } else {
                    candidates.remove(DataCandidate.JSONB);
                }
            }
        }

        String inferType() {
            if (nonEmpty == 0) {
                return "text";
            }
            if (candidates.contains(DataCandidate.BOOLEAN)) return "boolean";
            if (candidates.contains(DataCandidate.NUMERIC)) return "numeric";
            if (candidates.contains(DataCandidate.INTEGER)) return "integer";
            if (candidates.contains(DataCandidate.BIGINT)) return "bigint";
            if (candidates.contains(DataCandidate.DATE)) return "date";
            if (candidates.contains(DataCandidate.TIMESTAMP)) return "timestamp";
            if (candidates.contains(DataCandidate.UUID)) return "uuid";
            if (candidates.contains(DataCandidate.JSONB) && jsonOk > 0 && nonEmpty >= 3 && jsonOk * 100 >= nonEmpty * 95) {
                return "jsonb";
            }
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
            try {
                JsonParser.parseString(v);
                return true;
            } catch (JsonParseException | IllegalStateException e) {
                return false;
            }
        }

        private boolean looksLikeJson(String v) {
            String trimmed = v.trim();
            return trimmed.startsWith("{") || trimmed.startsWith("[");
        }
    }

    private enum DataCandidate {
        BOOLEAN, INTEGER, BIGINT, NUMERIC, DATE, TIMESTAMP, UUID, JSONB, VARCHAR, TEXT
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
