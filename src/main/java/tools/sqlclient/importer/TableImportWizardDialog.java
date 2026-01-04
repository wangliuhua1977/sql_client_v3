package tools.sqlclient.importer;

import javax.swing.*;
import java.awt.*;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.event.ActionEvent;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A lightweight wizard dialog that guides users to import tabular data into PostgreSQL.
 * This implementation focuses on UI scaffolding and lifecycle safety; heavy lifting
 * is delegated to helper classes that stream data and perform inserts.
 */
public class TableImportWizardDialog extends JDialog {
    private final CardLayout cardLayout = new CardLayout();
    private final JPanel cards = new JPanel(cardLayout);
    private final JButton backButton = new JButton("上一步");
    private final JButton nextButton = new JButton("下一步");
    private final JButton startButton = new JButton("开始导入");
    private final JButton cancelButton = new JButton("取消");
    private final JTextArea pasteArea = new JTextArea();
    private final JTextField csvPathField = new JTextField();
    private final JTextField xlsxPathField = new JTextField();
    private final JTextArea reportArea = new JTextArea();
    private final JProgressBar progressBar = new JProgressBar();
    private final JLabel statusLabel = new JLabel("等待开始");
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private int step = 0;

    public TableImportWizardDialog(Frame owner) {
        super(owner, "表格导入向导", false);
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        setPreferredSize(new Dimension(900, 640));
        setLayout(new BorderLayout());

        cards.add(buildStep0(), "step0");
        cards.add(buildStep1(), "step1");
        cards.add(buildStep2(), "step2");
        cards.add(buildStep3(), "step3");

        JPanel footer = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        backButton.addActionListener(e -> prev());
        nextButton.addActionListener(e -> next());
        startButton.addActionListener(this::startImport);
        cancelButton.addActionListener(e -> cancelImport());
        footer.add(backButton);
        footer.add(nextButton);
        footer.add(startButton);
        footer.add(cancelButton);

        add(cards, BorderLayout.CENTER);
        add(footer, BorderLayout.SOUTH);
        pack();
        setLocationRelativeTo(owner);
        updateButtons();
    }

    private JPanel buildStep0() {
        JPanel panel = new JPanel(new BorderLayout());
        ButtonGroup group = new ButtonGroup();
        JRadioButton paste = new JRadioButton("粘贴", true);
        JRadioButton csv = new JRadioButton("CSV 文件");
        JRadioButton xlsx = new JRadioButton("XLSX 文件");
        group.add(paste);
        group.add(csv);
        group.add(xlsx);
        JPanel options = new JPanel(new GridLayout(3, 1));
        options.add(paste);
        options.add(csv);
        options.add(xlsx);
        panel.add(new JLabel("选择数据源："), BorderLayout.NORTH);
        panel.add(options, BorderLayout.CENTER);
        panel.putClientProperty("pasteRadio", paste);
        panel.putClientProperty("csvRadio", csv);
        panel.putClientProperty("xlsxRadio", xlsx);
        return panel;
    }

    private JPanel buildStep1() {
        JPanel panel = new JPanel(new BorderLayout());
        JTabbedPane tabs = new JTabbedPane();
        JScrollPane pasteScroll = new JScrollPane(pasteArea);
        pasteArea.setLineWrap(true);
        tabs.addTab("粘贴", pasteScroll);

        JPanel csvPanel = new JPanel(new BorderLayout(5, 5));
        JButton csvBrowse = new JButton("选择 CSV...");
        csvBrowse.addActionListener(e -> chooseFile(csvPathField));
        csvPanel.add(csvPathField, BorderLayout.CENTER);
        csvPanel.add(csvBrowse, BorderLayout.EAST);
        tabs.addTab("CSV", csvPanel);

        JPanel xlsxPanel = new JPanel(new BorderLayout(5, 5));
        JButton xlsxBrowse = new JButton("选择 XLSX...");
        xlsxBrowse.addActionListener(e -> chooseFile(xlsxPathField));
        xlsxPanel.add(xlsxPathField, BorderLayout.CENTER);
        xlsxPanel.add(xlsxBrowse, BorderLayout.EAST);
        tabs.addTab("XLSX", xlsxPanel);

        panel.add(new JLabel("配置数据源并读取预览。"), BorderLayout.NORTH);
        panel.add(tabs, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildStep2() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.add(new JLabel("预览与字段推断（简化预览，仅展示部分行）。"), BorderLayout.NORTH);
        JTable table = new JTable(new Object[][]{{"预览尚未实现"}}, new String[]{"Preview"});
        panel.add(new JScrollPane(table), BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildStep3() {
        JPanel panel = new JPanel(new BorderLayout());
        JPanel top = new JPanel(new BorderLayout());
        top.add(statusLabel, BorderLayout.WEST);
        top.add(progressBar, BorderLayout.CENTER);
        panel.add(top, BorderLayout.NORTH);
        reportArea.setEditable(false);
        panel.add(new JScrollPane(reportArea), BorderLayout.CENTER);
        return panel;
    }

    private void chooseFile(JTextField target) {
        JFileChooser chooser = new JFileChooser();
        if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            target.setText(chooser.getSelectedFile().getAbsolutePath());
        }
    }

    private void next() {
        if (step < 3) {
            step++;
            cardLayout.show(cards, "step" + step);
            updateButtons();
        }
    }

    private void prev() {
        if (step > 0) {
            step--;
            cardLayout.show(cards, "step" + step);
            updateButtons();
        }
    }

    private void updateButtons() {
        backButton.setEnabled(step > 0);
        nextButton.setEnabled(step < 2);
        startButton.setEnabled(step == 2);
        cancelButton.setEnabled(step >= 2);
    }

    private void startImport(ActionEvent e) {
        step = 3;
        cardLayout.show(cards, "step3");
        updateButtons();
        cancelled.set(false);
        statusLabel.setText("导入中…");
        progressBar.setIndeterminate(true);
        new Thread(() -> {
            try {
                simulateImport();
            } finally {
                SwingUtilities.invokeLater(() -> progressBar.setIndeterminate(false));
            }
        }, "import-preview-thread").start();
    }

    private void simulateImport() {
        ImportReport report = new ImportReport();
        report.setSourceType("PASTE/CSV/XLSX");
        report.setTableName("demo_table");
        try {
            Thread.sleep(1200);
            if (cancelled.get()) {
                report.setCancelled(true);
            } else {
                report.setInsertedRowCount(0);
                report.setSourceRowCount(0);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            report.setCancelled(true);
        }
        SwingUtilities.invokeLater(() -> {
            statusLabel.setText(report.isCancelled() ? "已取消" : "完成");
            reportArea.setText(report.toText());
        });
    }

    private void cancelImport() {
        cancelled.set(true);
        statusLabel.setText("已请求取消");
    }

    public static void showDialog(Frame owner) {
        TableImportWizardDialog dialog = new TableImportWizardDialog(owner);
        dialog.setVisible(true);
    }

    public static String readClipboardText() {
        try {
            return (String) Toolkit.getDefaultToolkit().getSystemClipboard().getData(DataFlavor.stringFlavor);
        } catch (UnsupportedFlavorException | IOException e) {
            return "";
        }
    }
}
