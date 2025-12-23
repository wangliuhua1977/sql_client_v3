package tools.sqlclient.ui;

import org.eclipse.lsp4j.Diagnostic;
import tools.sqlclient.lsp.pg.DiagnosticsStore;
import tools.sqlclient.lsp.pg.OffsetMapper;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ProblemsPanel extends JPanel {
    private final ProblemsTableModel model = new ProblemsTableModel();
    private Consumer<ProblemRow> onNavigate;

    public ProblemsPanel() {
        super(new BorderLayout());
        JTable table = new JTable(model);
        table.setFillsViewportHeight(true);
        table.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                if (e.getClickCount() == 2) {
                    int row = table.getSelectedRow();
                    if (row >= 0 && row < model.rows.size() && onNavigate != null) {
                        onNavigate.accept(model.rows.get(row));
                    }
                }
            }
        });
        add(new JScrollPane(table), BorderLayout.CENTER);
    }

    public void bindStore(DiagnosticsStore store, String uri, java.util.function.Supplier<String> textSupplier) {
        store.addListener((u, list) -> {
            if (!uri.equals(u)) return;
            SwingUtilities.invokeLater(() -> model.update(list, textSupplier.get()));
        });
    }

    public void setOnNavigate(Consumer<ProblemRow> onNavigate) {
        this.onNavigate = onNavigate;
    }

    public record ProblemRow(String severity, String message, int line, int column, int offset) { }

    private static class ProblemsTableModel extends AbstractTableModel {
        private final String[] columns = {"Severity", "Message", "Line", "Column"};
        private final List<ProblemRow> rows = new ArrayList<>();

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return columns.length;
        }

        @Override
        public String getColumnName(int column) {
            return columns[column];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            ProblemRow row = rows.get(rowIndex);
            return switch (columnIndex) {
                case 0 -> row.severity();
                case 1 -> row.message();
                case 2 -> row.line();
                case 3 -> row.column();
                default -> "";
            };
        }

        void update(List<Diagnostic> diagnostics, String text) {
            rows.clear();
            if (diagnostics != null && text != null) {
                OffsetMapper mapper = new OffsetMapper(text);
                for (Diagnostic d : diagnostics) {
                    int line = d.getRange().getStart().getLine() + 1;
                    int col = d.getRange().getStart().getCharacter() + 1;
                    int offset = mapper.toOffset(d.getRange().getStart().getLine(),
                            d.getRange().getStart().getCharacter(), text.length());
                    String sev = d.getSeverity() == null ? "Info" : switch (d.getSeverity()) {
                        case Error -> "Error";
                        case Warning -> "Warning";
                        default -> d.getSeverity().toString();
                    };
                    rows.add(new ProblemRow(sev, d.getMessage(), line, col, offset));
                }
            }
            fireTableDataChanged();
        }
    }
}
