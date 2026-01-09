package tools.sqlclient.ui;

import tools.sqlclient.exec.ColumnDef;

import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于 ColumnDef 的表格模型，允许重复列名，并使用列 id 作为内部标识。
 */
public class ColumnarTableModel extends AbstractTableModel {
    private List<ColumnDef> columns = List.of();
    private List<Object[]> rows = List.of();

    public void setData(List<ColumnDef> columns, List<List<String>> rows) {
        this.columns = columns == null ? List.of() : new ArrayList<>(columns);
        if (rows == null) {
            this.rows = List.of();
        } else {
            List<Object[]> converted = new ArrayList<>(rows.size());
            int columnCount = this.columns.size();
            for (List<String> row : rows) {
                Object[] values = new Object[columnCount];
                if (row != null) {
                    int limit = Math.min(row.size(), columnCount);
                    for (int i = 0; i < limit; i++) {
                        values[i] = row.get(i);
                    }
                }
                converted.add(values);
            }
            this.rows = converted;
        }
        fireTableStructureChanged();
    }

    public List<ColumnDef> getColumnDefs() {
        return columns;
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int column) {
        if (column < 0 || column >= columns.size()) {
            return "";
        }
        return columns.get(column).getDisplayName();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return null;
        }
        Object[] row = rows.get(rowIndex);
        if (row == null || columnIndex < 0 || columnIndex >= row.length) {
            return null;
        }
        return row[columnIndex];
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return columnIndex >= 0 && columnIndex < getColumnCount();
    }

    @Override
    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return;
        }
        Object[] row = rows.get(rowIndex);
        if (row == null || columnIndex < 0 || columnIndex >= row.length) {
            return;
        }
        row[columnIndex] = aValue;
        fireTableCellUpdated(rowIndex, columnIndex);
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return Object.class;
    }
}
