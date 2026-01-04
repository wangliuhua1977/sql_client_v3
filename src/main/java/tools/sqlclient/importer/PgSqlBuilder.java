package tools.sqlclient.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class PgSqlBuilder {
    public String buildCreateTableSql(String schema, String table, List<ColumnSpec> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SCHEMA IF NOT EXISTS ").append(schema).append(";\n");
        sb.append("CREATE TABLE ").append(schema).append('.').append(table).append(" (");
        StringJoiner joiner = new StringJoiner(", ");
        for (ColumnSpec col : columns) {
            joiner.add(col.getNormalizedName() + " " + col.getPgType());
        }
        sb.append(joiner).append(");");
        return sb.toString();
    }

    public List<String> buildInsertBatches(String schema, String table, List<ColumnSpec> columns, List<List<String>> rows, boolean emptyAsNull, int batchSize) {
        List<String> batches = new ArrayList<>();
        int total = rows.size();
        for (int i = 0; i < total; i += batchSize) {
            List<List<String>> subset = rows.subList(i, Math.min(total, i + batchSize));
            batches.add(buildInsertBatch(schema, table, columns, subset, emptyAsNull));
        }
        return batches;
    }

    private String buildInsertBatch(String schema, String table, List<ColumnSpec> columns, List<List<String>> rows, boolean emptyAsNull) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(schema).append('.').append(table).append(" (");
        StringJoiner cols = new StringJoiner(", ");
        for (ColumnSpec c : columns) {
            cols.add(c.getNormalizedName());
        }
        sb.append(cols).append(") VALUES ");
        StringJoiner valuesJoiner = new StringJoiner(", ");
        for (List<String> row : rows) {
            valuesJoiner.add(formatRow(row, columns, emptyAsNull));
        }
        sb.append(valuesJoiner).append(";");
        return sb.toString();
    }

    private String formatRow(List<String> row, List<ColumnSpec> columns, boolean emptyAsNull) {
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < columns.size(); i++) {
            String value = i < row.size() ? row.get(i) : "";
            joiner.add(formatValue(value, columns.get(i).getPgType(), emptyAsNull));
        }
        return joiner.toString();
    }

    private String formatValue(String value, String type, boolean emptyAsNull) {
        if (value == null) {
            return "NULL";
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty() && emptyAsNull) {
            return "NULL";
        }
        switch (type) {
            case "boolean":
                return Boolean.parseBoolean(trimmed.toLowerCase()) || "1".equals(trimmed) || "t".equalsIgnoreCase(trimmed) ? "TRUE" : "FALSE";
            case "integer":
            case "bigint":
            case "numeric":
                return trimmed;
            case "date":
                return "DATE '" + escapeSql(trimmed) + "'";
            case "timestamp":
                return "TIMESTAMP '" + escapeSql(trimmed) + "'";
            default:
                return "'" + escapeSql(trimmed) + "'";
        }
    }

    private String escapeSql(String text) {
        if (text.indexOf('\n') >= 0 || text.indexOf('\t') >= 0 || text.indexOf('\r') >= 0) {
            return text.replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r");
        }
        return text.replace("'", "''");
    }
}
