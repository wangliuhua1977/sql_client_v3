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
    private List<List<String>> rows = List.of();

    public void setData(List<ColumnDef> columns, List<List<String>> rows) {
        this.columns = columns == null ? List.of() : new ArrayList<>(columns);
        this.rows = rows == null ? List.of() : new ArrayList<>(rows);
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
        List<String> row = rows.get(rowIndex);
        if (row == null || columnIndex < 0 || columnIndex >= row.size()) {
            return null;
        }
        return row.get(columnIndex);
    }
}
