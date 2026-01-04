package tools.sqlclient.importer;

import tools.sqlclient.exec.SqlExecutionService;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 表格导入向导（简化实现）。
 */
public class TableImportWizardDialog extends JDialog {
    private final CardLayout cardLayout = new CardLayout();
    private final JPanel cardPanel = new JPanel(cardLayout);
    private final JTextArea pasteArea = new JTextArea(10, 80);
    private final JTextField csvField = new JTextField();
    private final JTextField xlsxField = new JTextField();
    private final JTextField sheetField = new JTextField("Sheet1");
    private final JTable previewTable = new JTable();
    private final JTextField tableNameField = new JTextField("import_table");
    private final JComboBox<ImportExecutionService.Mode> modeBox = new JComboBox<>(ImportExecutionService.Mode.values());
    private RowSource currentSource;
    private List<ColumnSpec> inferredColumns = new ArrayList<>();
    private final JTextArea logArea = new JTextArea();
    private final JTextArea reportArea = new JTextArea();
    private final ImportExecutionService importService = new ImportExecutionService();
    private final ImportAuditService auditService = new ImportAuditService();
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);
    private final SqlExecutionService sqlExecutionService = new SqlExecutionService();

    public TableImportWizardDialog(Frame owner) {
        super(owner, "表格导入到PG(粘贴/CSV/XLSX)", false);
        setSize(900, 700);
        setLocationRelativeTo(owner);
        buildSteps();
    }

    private void buildSteps() {
        cardPanel.add(buildStep0(), "step0");
        cardPanel.add(buildStep1(), "step1");
        cardPanel.add(buildStep2(), "step2");
        cardPanel.add(buildStep3(), "step3");
        setLayout(new BorderLayout());
        add(cardPanel, BorderLayout.CENTER);
        cardLayout.show(cardPanel, "step0");
    }

    private JPanel buildStep0() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        JPanel options = new JPanel(new GridLayout(3, 1, 8, 8));
        ButtonGroup group = new ButtonGroup();
        JRadioButton paste = new JRadioButton("粘贴", true);
        JRadioButton csv = new JRadioButton("CSV 文件");
        JRadioButton xlsx = new JRadioButton("XLSX 文件");
        group.add(paste);
        group.add(csv);
        group.add(xlsx);
        options.add(paste);
        options.add(csv);
        options.add(xlsx);
        JButton next = new JButton("下一步");
        next.addActionListener(e -> {
            if (paste.isSelected()) {
                cardLayout.show(cardPanel, "step1");
                cardPanel.putClientProperty("sourceType", "PASTE");
            } else if (csv.isSelected()) {
                cardLayout.show(cardPanel, "step1");
                cardPanel.putClientProperty("sourceType", "CSV");
            } else {
                cardLayout.show(cardPanel, "step1");
                cardPanel.putClientProperty("sourceType", "XLSX");
            }
        });
        panel.add(options, BorderLayout.CENTER);
        panel.add(next, BorderLayout.SOUTH);
        return panel;
    }

    private JPanel buildStep1() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        JTabbedPane tabs = new JTabbedPane();
        tabs.addTab("粘贴", new JScrollPane(pasteArea));
        tabs.addTab("CSV", buildCsvPanel());
        tabs.addTab("XLSX", buildXlsxPanel());
        panel.add(tabs, BorderLayout.CENTER);
        JButton next = new JButton("读取并预览");
        next.addActionListener(this::handleReadSample);
        panel.add(next, BorderLayout.SOUTH);
        return panel;
    }

    private JPanel buildCsvPanel() {
        JPanel p = new JPanel(new BorderLayout(6, 6));
        JButton choose = new JButton("选择 CSV 文件");
        choose.addActionListener(e -> chooseFile(csvField));
        p.add(choose, BorderLayout.WEST);
        p.add(csvField, BorderLayout.CENTER);
        return p;
    }

    private JPanel buildXlsxPanel() {
        JPanel p = new JPanel(new GridLayout(3, 1, 6, 6));
        JPanel row1 = new JPanel(new BorderLayout(6, 6));
        JButton choose = new JButton("选择 XLSX 文件");
        choose.addActionListener(e -> chooseFile(xlsxField));
        row1.add(choose, BorderLayout.WEST);
        row1.add(xlsxField, BorderLayout.CENTER);
        JPanel row2 = new JPanel(new BorderLayout(6, 6));
        row2.add(new JLabel("Sheet 名称"), BorderLayout.WEST);
        row2.add(sheetField, BorderLayout.CENTER);
        p.add(row1);
        p.add(row2);
        return p;
    }

    private JPanel buildStep2() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        panel.add(new JScrollPane(previewTable), BorderLayout.CENTER);
        JPanel bottom = new JPanel(new GridLayout(2, 2, 6, 6));
        bottom.add(new JLabel("表名"));
        bottom.add(tableNameField);
        bottom.add(new JLabel("模式"));
        bottom.add(modeBox);
        JButton next = new JButton("开始导入");
        next.addActionListener(e -> cardLayout.show(cardPanel, "step3"));
        panel.add(bottom, BorderLayout.NORTH);
        panel.add(next, BorderLayout.SOUTH);
        return panel;
    }

    private JPanel buildStep3() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));
        logArea.setEditable(false);
        reportArea.setEditable(false);
        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(logArea), new JScrollPane(reportArea));
        split.setResizeWeight(0.6);
        panel.add(split, BorderLayout.CENTER);
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton start = new JButton("开始");
        JButton cancel = new JButton("取消");
        start.addActionListener(e -> startImport());
        cancel.addActionListener(e -> cancelFlag.set(true));
        buttons.add(start);
        buttons.add(cancel);
        panel.add(buttons, BorderLayout.SOUTH);
        return panel;
    }

    private void chooseFile(JTextField field) {
        JFileChooser chooser = new JFileChooser();
        int ret = chooser.showOpenDialog(this);
        if (ret == JFileChooser.APPROVE_OPTION) {
            field.setText(chooser.getSelectedFile().getAbsolutePath());
        }
    }

    private void handleReadSample(ActionEvent e) {
        String type = (String) cardPanel.getClientProperty("sourceType");
        try {
            if ("PASTE".equals(type)) {
                currentSource = RowSourceFactory.forClipboard(pasteArea.getText());
            } else if ("CSV".equals(type)) {
                currentSource = RowSourceFactory.forCsv(Path.of(csvField.getText()), Charset.forName("UTF-8"), ',', '"');
            } else {
                currentSource = RowSourceFactory.forXlsx(Path.of(xlsxField.getText()), sheetField.getText());
            }
            currentSource.open();
            List<String> headers = currentSource.getHeaders();
            List<List<String>> sampleRows = new ArrayList<>();
            RowData row;
            int maxPreview = 50;
            while ((row = currentSource.nextRow()) != null && sampleRows.size() < maxPreview) {
                sampleRows.add(row.getValues());
            }
            inferredColumns = new TypeInferer().infer(headers, sampleRows);
            fillPreviewTable(headers, sampleRows);
            cardLayout.show(cardPanel, "step2");
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "读取失败: " + ex.getMessage(), "错误", JOptionPane.ERROR_MESSAGE);
        } finally {
            try {
                if (currentSource != null) {
                    currentSource.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    private void fillPreviewTable(List<String> headers, List<List<String>> rows) {
        DefaultTableModel model = new DefaultTableModel();
        for (String h : headers) {
            model.addColumn(h);
        }
        for (List<String> row : rows) {
            model.addRow(row.toArray());
        }
        previewTable.setModel(model);
    }

    private void startImport() {
        cancelFlag.set(false);
        if (currentSource == null) {
            JOptionPane.showMessageDialog(this, "请先读取数据", "错误", JOptionPane.ERROR_MESSAGE);
            return;
        }
        new Thread(() -> {
            try {
                ImportExecutionService.Mode mode = (ImportExecutionService.Mode) modeBox.getSelectedItem();
                importService.execute(currentSource, inferredColumns, "leshan", tableNameField.getText(), mode, List.of(),
                        msg -> SwingUtilities.invokeLater(() -> logArea.append(msg + "\n")),
                        report -> SwingUtilities.invokeLater(() -> showReport(mode, report)),
                        cancelFlag);
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, "导入失败: " + ex.getMessage(), "错误", JOptionPane.ERROR_MESSAGE));
            }
        }, "import-launcher").start();
    }

    private void showReport(ImportExecutionService.Mode mode, ImportReport report) {
        boolean pass = auditService.auditCounts(mode, report);
        String text = report.format() + "稽核: " + (pass ? "PASS" : "FAIL");
        reportArea.setText(text);
    }
}
