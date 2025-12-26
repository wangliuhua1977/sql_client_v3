package tools.sqlclient.ui.table;

import javax.swing.*;
import javax.swing.table.TableColumn;
import java.awt.*;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 提供 JTable 选区、列值及列名复制的工具方法。
 */
public final class TableCopySupport {
    private TableCopySupport() {
    }

    public static String buildCellSelectionTsv(JTable table) {
        if (table == null) {
            return "";
        }
        int[] selectedRows = table.getSelectedRows();
        int[] selectedCols = table.getSelectedColumns();
        if (selectedRows.length == 0 || selectedCols.length == 0) {
            return "";
        }
        Arrays.sort(selectedRows);
        Arrays.sort(selectedCols);
        StringBuilder sb = new StringBuilder();
        boolean firstRowAppended = false;
        for (int row : selectedRows) {
            StringBuilder rowBuilder = new StringBuilder();
            boolean hasCellInRow = false;
            for (int col : selectedCols) {
                if (!table.isCellSelected(row, col)) {
                    continue;
                }
                if (hasCellInRow) {
                    rowBuilder.append('\t');
                }
                Object value = table.getValueAt(row, col);
                rowBuilder.append(value == null ? "" : String.valueOf(value));
                hasCellInRow = true;
            }
            if (hasCellInRow) {
                if (firstRowAppended) {
                    sb.append('\n');
                }
                sb.append(rowBuilder);
                firstRowAppended = true;
            }
        }
        return sb.toString();
    }

    public static String buildSelectedColumnValuesTsv(JTable table, List<Integer> viewColumns) {
        if (table == null || viewColumns == null || viewColumns.isEmpty()) {
            return "";
        }
        List<Integer> cols = sanitizedColumns(viewColumns);
        StringBuilder sb = new StringBuilder();
        for (int row = 0; row < table.getRowCount(); row++) {
            if (row > 0) {
                sb.append('\n');
            }
            boolean firstCol = true;
            for (int col : cols) {
                if (!firstCol) {
                    sb.append('\t');
                }
                Object value = table.getValueAt(row, col);
                sb.append(value == null ? "" : String.valueOf(value));
                firstCol = false;
            }
        }
        return sb.toString();
    }

    public static String buildSelectedColumnNamesCsv(JTable table, List<Integer> viewColumns) {
        if (table == null || viewColumns == null || viewColumns.isEmpty()) {
            return "";
        }
        List<Integer> cols = sanitizedColumns(viewColumns);
        Set<String> names = new LinkedHashSet<>();
        for (int col : cols) {
            TableColumn tableColumn = table.getColumnModel().getColumn(col);
            Object headerValue = tableColumn.getHeaderValue();
            if (headerValue != null && !headerValue.toString().isBlank()) {
                names.add(headerValue.toString());
                continue;
            }
            int modelIndex = table.convertColumnIndexToModel(col);
            String modelName = table.getModel().getColumnName(modelIndex);
            names.add(modelName == null ? "" : modelName);
        }
        return names.stream().collect(Collectors.joining(", "));
    }

    public static void copyToClipboard(String text) {
        Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
        StringSelection selection = new StringSelection(text);
        clipboard.setContents(selection, selection);
    }

    private static List<Integer> sanitizedColumns(List<Integer> viewColumns) {
        List<Integer> cols = new ArrayList<>(new LinkedHashSet<>(viewColumns));
        cols.sort(Integer::compareTo);
        return cols;
    }
}
