package tools.sqlclient.importer;

import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionException;
import tools.sqlclient.exec.SqlExecutionService;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ExcelPasteImportWizardDialog extends JDialog {
    private final CardLayout cardLayout = new CardLayout();
    private final JPanel cardPanel = new JPanel(cardLayout);
    private final JTextArea pasteArea = new JTextArea(10, 80);
    private final JLabel parseErrorLabel = new JLabel();
    private final JTable previewTable = new JTable();
    private final DefaultTableModel previewModel = new DefaultTableModel();
    private final JPanel fieldEditorPanel = new JPanel();
    private final ButtonGroup modeGroup = new ButtonGroup();
    private final JRadioButton modeCreate = new JRadioButton("新建表", true);
    private final JRadioButton modeTruncate = new JRadioButton("清空并插入");
    private final JRadioButton modeAppend = new JRadioButton("追加插入");
    private final JCheckBox emptyAsNull = new JCheckBox("空字符串视为 NULL", true);
    private final JTextField tableNameField = new JTextField(30);
    private final JLabel schemaLabel = new JLabel("leshan");
    private final JLabel summaryLabel = new JLabel();
    private final JTextArea logArea = new JTextArea();
    private final JProgressBar progressBar = new JProgressBar();

    private TableData parsed;
    private List<ColumnSpec> columnSpecs = new ArrayList<>();
    private final ColumnNameNormalizer normalizer = new ColumnNameNormalizer();
    private final TypeInferer typeInferer = new TypeInferer();
    private final PasteImportService importService;

    public ExcelPasteImportWizardDialog(Window owner, SqlExecutionService sqlExecutionService) {
        super(owner);
        this.importService = new PasteImportService(sqlExecutionService);
        setTitle("Excel 粘贴导入到PG");
        setModal(false);
        setSize(900, 700);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout());
        buildCards();
    }

    private void buildCards() {
        cardPanel.add(buildStep1(), "step1");
        cardPanel.add(buildStep2(), "step2");
        cardPanel.add(buildStep3(), "step3");
        add(cardPanel, BorderLayout.CENTER);
    }

    private JPanel buildStep1() {
        JPanel panel = new JPanel(new BorderLayout());
        JPanel top = new JPanel(new BorderLayout());
        top.add(new JLabel("从 Excel/WPS 复制区域（含标题行）后直接粘贴"), BorderLayout.NORTH);
        parseErrorLabel.setForeground(Color.RED);
        top.add(parseErrorLabel, BorderLayout.SOUTH);
        panel.add(top, BorderLayout.NORTH);
        pasteArea.setLineWrap(true);
        pasteArea.setWrapStyleWord(true);
        panel.add(new JScrollPane(pasteArea), BorderLayout.CENTER);
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton next = new JButton(new AbstractAction("下一步") {
            @Override
            public void actionPerformed(ActionEvent e) {
                onParseNext();
            }
        });
        buttons.add(next);
        panel.add(buttons, BorderLayout.SOUTH);
        return panel;
    }

    private JPanel buildStep2() {
        JPanel panel = new JPanel(new BorderLayout());
        previewTable.setModel(previewModel);
        panel.add(new JScrollPane(previewTable), BorderLayout.CENTER);

        fieldEditorPanel.setLayout(new BoxLayout(fieldEditorPanel, BoxLayout.Y_AXIS));
        panel.add(new JScrollPane(fieldEditorPanel), BorderLayout.EAST);

        JPanel params = new JPanel(new GridLayout(0, 2, 8, 8));
        params.setBorder(BorderFactory.createTitledBorder("参数"));
        params.add(new JLabel("Schema"));
        params.add(schemaLabel);
        params.add(new JLabel("表名"));
        params.add(tableNameField);
        params.add(new JLabel("空值规则"));
        params.add(emptyAsNull);
        params.add(new JLabel("写入模式"));
        JPanel modePanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        modeGroup.add(modeCreate);
        modeGroup.add(modeTruncate);
        modeGroup.add(modeAppend);
        modePanel.add(modeCreate);
        modePanel.add(modeTruncate);
        modePanel.add(modeAppend);
        params.add(modePanel);
        panel.add(params, BorderLayout.SOUTH);

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton prev = new JButton("上一步");
        prev.addActionListener(e -> cardLayout.show(cardPanel, "step1"));
        JButton next = new JButton("下一步");
        next.addActionListener(e -> showStep3());
        buttons.add(prev);
        buttons.add(next);
        panel.add(buttons, BorderLayout.NORTH);
        return panel;
    }

    private JPanel buildStep3() {
        JPanel panel = new JPanel(new BorderLayout());
        JPanel top = new JPanel(new BorderLayout());
        top.add(summaryLabel, BorderLayout.CENTER);
        panel.add(top, BorderLayout.NORTH);

        logArea.setEditable(false);
        panel.add(new JScrollPane(logArea), BorderLayout.CENTER);
        panel.add(progressBar, BorderLayout.SOUTH);

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton start = new JButton("开始导入");
        start.addActionListener(e -> startImport());
        JButton close = new JButton("关闭");
        close.addActionListener(e -> dispose());
        buttons.add(start);
        buttons.add(close);
        panel.add(buttons, BorderLayout.WEST);
        return panel;
    }

    private void onParseNext() {
        parseErrorLabel.setText("");
        ClipboardTableParser parser = new ClipboardTableParser();
        try {
            parsed = parser.parse(pasteArea.getText());
        } catch (ParseError ex) {
            parseErrorLabel.setText(ex.getMessage());
            return;
        }
        List<String> normalized = normalizer.normalize(parsed.getRawHeaders());
        List<String> inferred = typeInferer.infer(parsed, emptyAsNull.isSelected());
        columnSpecs = new ArrayList<>();
        for (int i = 0; i < normalized.size(); i++) {
            columnSpecs.add(new ColumnSpec(parsed.getRawHeaders().get(i), normalized.get(i), inferred.get(i)));
        }
        refreshPreview();
        tableNameField.setText("paste_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
        cardLayout.show(cardPanel, "step2");
    }

    private void refreshPreview() {
        previewModel.setColumnCount(0);
        previewModel.setRowCount(0);
        for (String h : parsed.getRawHeaders()) {
            previewModel.addColumn(h);
        }
        int max = Math.min(50, parsed.getRows().size());
        for (int i = 0; i < max; i++) {
            List<String> row = parsed.getRows().get(i);
            previewModel.addRow(row.toArray());
        }
        renderFieldEditors();
    }

    private void renderFieldEditors() {
        fieldEditorPanel.removeAll();
        for (ColumnSpec spec : columnSpecs) {
            JPanel row = new JPanel(new GridLayout(1, 3, 4, 4));
            JTextField nameField = new JTextField(spec.getNormalizedName());
            JComboBox<String> typeBox = new JComboBox<>(new String[]{"boolean", "integer", "bigint", "numeric", "date", "timestamp", "text"});
            typeBox.setSelectedItem(spec.getPgType());
            row.add(new JLabel(spec.getOriginalName()));
            row.add(nameField);
            row.add(typeBox);
            nameField.getDocument().addDocumentListener(new SimpleChangeListener(() -> spec.setNormalizedName(nameField.getText().trim())));
            typeBox.addActionListener(e -> spec.setPgType((String) typeBox.getSelectedItem()));
            fieldEditorPanel.add(row);
        }
        fieldEditorPanel.revalidate();
        fieldEditorPanel.repaint();
    }

    private void showStep3() {
        summaryLabel.setText(String.format("表: %s.%s, 列: %d, 行: %d", schemaLabel.getText(), tableNameField.getText(), columnSpecs.size(), parsed.getRowCount()));
        cardLayout.show(cardPanel, "step3");
    }

    private void startImport() {
        progressBar.setIndeterminate(true);
        logArea.append("开始导入...\n");
        String schema = schemaLabel.getText();
        String table = tableNameField.getText().trim();
        CompletableFuture<SqlExecResult> future;
        if (modeCreate.isSelected()) {
            future = importService.createAndInsert(schema, table, columnSpecs, parsed.getRows(), emptyAsNull.isSelected());
        } else if (modeTruncate.isSelected()) {
            future = importService.truncateAndInsert(schema, table, columnSpecs, parsed.getRows(), emptyAsNull.isSelected());
        } else {
            future = importService.appendInsert(schema, table, columnSpecs, parsed.getRows(), emptyAsNull.isSelected());
        }
        future.whenComplete((res, err) -> SwingUtilities.invokeLater(() -> {
            progressBar.setIndeterminate(false);
            if (err != null) {
                logArea.append("失败: " + err.getMessage() + "\n");
                if (err instanceof SqlExecutionException se && se.getErrorInfo() != null) {
                    logArea.append(String.valueOf(se.getErrorInfo().getMessage())).append('\n');
                }
                return;
            }
            if (res != null && res.getError() != null) {
                logArea.append("失败: " + res.getError().getMessage() + "\n");
                return;
            }
            logArea.append("成功\n");
        }));
    }

    private static class SimpleChangeListener implements javax.swing.event.DocumentListener {
        private final Runnable runnable;

        SimpleChangeListener(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void insertUpdate(javax.swing.event.DocumentEvent e) {
            runnable.run();
        }

        @Override
        public void removeUpdate(javax.swing.event.DocumentEvent e) {
            runnable.run();
        }

        @Override
        public void changedUpdate(javax.swing.event.DocumentEvent e) {
            runnable.run();
        }
    }
}
