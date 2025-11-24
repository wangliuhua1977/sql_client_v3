package tools.sqlclient.exec;

import java.util.List;

/**
 * SQL 执行结果模型，包含列名、行数据与原始 SQL。
 */
public class SqlExecResult {
    private final String sql;
    private final List<String> columns;
    private final List<List<String>> rows;
    private final int rowsCount;

    public SqlExecResult(String sql, List<String> columns, List<List<String>> rows, int rowsCount) {
        this.sql = sql;
        this.columns = columns;
        this.rows = rows;
        this.rowsCount = rowsCount;
    }

    public String getSql() {
        return sql;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public int getRowsCount() {
        return rowsCount;
    }
}
