package tools.sqlclient.importer;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableImportWizardDialog extends JDialog {
    private static TableImportWizardDialog INSTANCE;

    public static void showDialog(Frame owner) {
        if (INSTANCE == null) {
            INSTANCE = new TableImportWizardDialog(owner);
        }
        INSTANCE.setLocationRelativeTo(owner);
        INSTANCE.setVisible(true);
        INSTANCE.toFront();
    }

    private final CardLayout cardLayout = new CardLayout();
    private final JPanel cardPanel = new JPanel(cardLayout);
    private final JRadioButton sourceClipboard = new JRadioButton("粘贴区域", true);
    private final JRadioButton sourceCsv = new JRadioButton("CSV 文件");
    private final JRadioButton sourceXlsx = new JRadioButton("XLSX 文件");

    private final JTextArea clipboardArea = new JTextArea();
    private final JTextField csvPathField = new JTextField();
    private final JComboBox<String> csvEncodingBox = new JComboBox<>(new String[]{"UTF-8", "GBK"});
    private final JTextField csvDelimiterField = new JTextField(",");
    private final JCheckBox csvHasHeader = new JCheckBox("首行是标题", true);

    private final JTextField xlsxPathField = new JTextField();
    private final JTextField xlsxSheetField = new JTextField();
    private final JSpinner xlsxHeaderRow = new JSpinner(new SpinnerNumberModel(0, 0, 10000, 1));
    private final JSpinner xlsxDataStartRow = new JSpinner(new SpinnerNumberModel(1, 0, 10000, 1));

    private final JTable previewTable = new JTable();
    private final JTextField tableNameField = new JTextField("import_table");
    private final JComboBox<ImportReport.Mode> modeBox = new JComboBox<>(ImportReport.Mode.values());
    private final JTextArea logArea = new JTextArea();
    private final JProgressBar progressBar = new JProgressBar();
    private final JTextArea reportArea = new JTextArea();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final ImportExecutionService executionService = new ImportExecutionService();
    private RowSource currentSource;
    private Future<?> runningTask;
    private String currentStep = "step0";

    private TableImportWizardDialog(Frame owner) {
        super(owner, "表格导入到PG(粘贴/CSV/XLSX)...", false);
        setSize(900, 650);
        setLayout(new BorderLayout());
        add(buildTop(), BorderLayout.NORTH);
        add(cardPanel, BorderLayout.CENTER);
        cardPanel.add(buildStep0(), "step0");
        cardPanel.add(buildStep1(), "step1");
        cardPanel.add(buildStep2(), "step2");
        cardPanel.add(buildStep3(), "step3");
        showStep("step0");
    }

    private JComponent buildTop() {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton prev = new JButton(new AbstractAction("上一步") {
            @Override
            public void actionPerformed(ActionEvent e) {
                cardLayout.previous(cardPanel);
            }
        });
        JButton next = new JButton(new AbstractAction("下一步") {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (cardLayoutCurrent().equals("step0")) {
                    showStep("step1");
                } else if (cardLayoutCurrent().equals("step1")) {
                    loadSourceAndPreview();
                } else if (cardLayoutCurrent().equals("step2")) {
                    showStep("step3");
                }
            }
        });
        JButton close = new JButton(new AbstractAction("关闭") {
            @Override
            public void actionPerformed(ActionEvent e) {
                setVisible(false);
            }
        });
        panel.add(prev);
        panel.add(next);
        panel.add(close);
        return panel;
    }

    private String cardLayoutCurrent() {
        return currentStep;
    }

    private void showStep(String step) {
        currentStep = step;
        cardLayout.show(cardPanel, step);
    }

    private JComponent buildStep0() {
        JPanel panel = new JPanel(new GridLayout(3, 1));
        ButtonGroup group = new ButtonGroup();
        group.add(sourceClipboard);
        group.add(sourceCsv);
        group.add(sourceXlsx);
        panel.add(sourceClipboard);
        panel.add(sourceCsv);
        panel.add(sourceXlsx);
        panel.setBorder(new EmptyBorder(12, 12, 12, 12));
        return panel;
    }

    private JComponent buildStep1() {
        JPanel panel = new JPanel(new CardLayout());
        panel.add(buildClipboardPane(), "clipboard");
        panel.add(buildCsvPane(), "csv");
        panel.add(buildXlsxPane(), "xlsx");
        // toggle card when selection change
        java.util.function.Consumer<String> switcher = type -> {
            CardLayout cl = (CardLayout) panel.getLayout();
            cl.show(panel, type);
        };
        sourceClipboard.addActionListener(e -> switcher.accept("clipboard"));
        sourceCsv.addActionListener(e -> switcher.accept("csv"));
        sourceXlsx.addActionListener(e -> switcher.accept("xlsx"));
        switcher.accept("clipboard");
        return panel;
    }

    private JComponent buildClipboardPane() {
        clipboardArea.setLineWrap(true);
        JScrollPane sp = new JScrollPane(clipboardArea);
        sp.setBorder(new EmptyBorder(8, 8, 8, 8));
        return sp;
    }

    private JComponent buildCsvPane() {
        JPanel p = new JPanel(new GridLayout(0, 1, 6, 6));
        JButton choose = new JButton("选择 CSV 文件");
        choose.addActionListener(e -> chooseFile(csvPathField));
        p.add(labeled("路径", row(csvPathField, choose)));
        p.add(labeled("编码", csvEncodingBox));
        p.add(labeled("分隔符", csvDelimiterField));
        p.add(csvHasHeader);
        p.setBorder(new EmptyBorder(8, 8, 8, 8));
        return new JScrollPane(p);
    }

    private JComponent buildXlsxPane() {
        JPanel p = new JPanel(new GridLayout(0, 1, 6, 6));
        JButton choose = new JButton("选择 XLSX 文件");
        choose.addActionListener(e -> chooseFile(xlsxPathField));
        p.add(labeled("路径", row(xlsxPathField, choose)));
        p.add(labeled("Sheet 名", xlsxSheetField));
        p.add(labeled("标题行号(0基)", xlsxHeaderRow));
        p.add(labeled("数据起始行(0基)", xlsxDataStartRow));
        p.setBorder(new EmptyBorder(8, 8, 8, 8));
        return new JScrollPane(p);
    }

    private JPanel row(JComponent a, JComponent b) {
        JPanel p = new JPanel(new BorderLayout(6, 6));
        p.add(a, BorderLayout.CENTER);
        p.add(b, BorderLayout.EAST);
        return p;
    }

    private JPanel labeled(String label, JComponent comp) {
        JPanel p = new JPanel(new BorderLayout());
        p.add(new JLabel(label), BorderLayout.WEST);
        p.add(comp, BorderLayout.CENTER);
        return p;
    }

    private JComponent buildStep2() {
        JPanel p = new JPanel(new BorderLayout());
        previewTable.setModel(new DefaultTableModel());
        p.add(new JScrollPane(previewTable), BorderLayout.CENTER);
        JPanel south = new JPanel(new GridLayout(0, 2, 6, 6));
        south.add(labeled("目标表名", tableNameField));
        south.add(labeled("写入模式", modeBox));
        p.add(south, BorderLayout.SOUTH);
        return p;
    }

    private JComponent buildStep3() {
        JPanel p = new JPanel(new BorderLayout());
        logArea.setEditable(false);
        reportArea.setEditable(false);
        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton start = new JButton("开始导入");
        JButton cancel = new JButton("取消");
        JButton export = new JButton("导出报告");
        start.addActionListener(e -> startImport());
        cancel.addActionListener(e -> cancelled.set(true));
        export.addActionListener(e -> exportReport());
        top.add(start);
        top.add(cancel);
        top.add(export);
        p.add(top, BorderLayout.NORTH);
        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(logArea), new JScrollPane(reportArea));
        split.setResizeWeight(0.6);
        p.add(split, BorderLayout.CENTER);
        p.add(progressBar, BorderLayout.SOUTH);
        return p;
    }

    private void chooseFile(JTextField target) {
        JFileChooser chooser = new JFileChooser();
        int result = chooser.showOpenDialog(this);
        if (result == JFileChooser.APPROVE_OPTION) {
            target.setText(chooser.getSelectedFile().getAbsolutePath());
        }
    }

    private void loadSourceAndPreview() {
        try {
            if (sourceClipboard.isSelected()) {
                currentSource = RowSourceFactory.fromClipboard(clipboardArea.getText());
            } else if (sourceCsv.isSelected()) {
                Path path = Path.of(csvPathField.getText());
                Charset cs = Charset.forName((String) csvEncodingBox.getSelectedItem());
                char delimiter = csvDelimiterField.getText().isEmpty() ? ',' : csvDelimiterField.getText().charAt(0);
                currentSource = RowSourceFactory.fromCsv(path, cs, delimiter, csvHasHeader.isSelected());
            } else {
                Path path = Path.of(xlsxPathField.getText());
                currentSource = RowSourceFactory.fromXlsx(path, xlsxSheetField.getText(), (Integer) xlsxHeaderRow.getValue(), (Integer) xlsxDataStartRow.getValue());
            }
            populatePreview();
            cardLayout.show(cardPanel, "step2");
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, ex.getMessage(), "解析失败", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void populatePreview() throws IOException {
        DefaultTableModel model = new DefaultTableModel();
        for (String h : currentSource.getHeader()) {
            model.addColumn(h);
        }
        RowSource previewSource = currentSource.reopen();
        int limit = 50;
        List<String> row;
        while (limit-- > 0 && (row = previewSource.nextRow()) != null) {
            model.addRow(row.toArray());
        }
        previewTable.setModel(model);
    }

    private void startImport() {
        if (currentSource == null) {
            JOptionPane.showMessageDialog(this, "请先读取数据源", "错误", JOptionPane.ERROR_MESSAGE);
            return;
        }
        cancelled.set(false);
        logArea.setText("");
        reportArea.setText("");
        progressBar.setIndeterminate(true);
        ImportReport.Mode mode = (ImportReport.Mode) modeBox.getSelectedItem();
        ImportExecutionService.ImportOptions options = new ImportExecutionService.ImportOptions(mode, tableNameField.getText());
        runningTask = executionService.execute(currentSource.reopen(), options, this::appendLog, this::onDone, cancelled);
        cardLayout.show(cardPanel, "step3");
    }

    private void appendLog(String msg) {
        SwingUtilities.invokeLater(() -> {
            logArea.append("[" + Instant.now() + "] " + msg + "\n");
        });
    }

    private void onDone(ImportReport report) {
        SwingUtilities.invokeLater(() -> {
            progressBar.setIndeterminate(false);
            reportArea.setText(report.toText());
        });
    }

    private void exportReport() {
        if (reportArea.getText().isEmpty()) {
            JOptionPane.showMessageDialog(this, "暂无报告", "提示", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        JFileChooser chooser = new JFileChooser();
        chooser.setSelectedFile(new java.io.File("import_report.txt"));
        if (chooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            try {
                java.nio.file.Files.writeString(chooser.getSelectedFile().toPath(), reportArea.getText(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this, "保存失败: " + e.getMessage(), "错误", JOptionPane.ERROR_MESSAGE);
            }
        }
    }
}
