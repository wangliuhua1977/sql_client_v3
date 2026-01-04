package tools.sqlclient.importer;

import java.util.List;
import java.util.StringJoiner;

public class PgSqlBuilder {
    public String buildCreateTable(String schema, String table, List<ColumnSpec> columns) {
        StringJoiner joiner = new StringJoiner(", ");
        for (ColumnSpec c : columns) {
            joiner.add(c.getNormalizedName() + " " + c.getInferredType());
        }
        return "CREATE TABLE " + schema + "." + table + " (" + joiner + ")";
    }

    public String buildTruncate(String schema, String table) {
        return "TRUNCATE TABLE " + schema + "." + table;
    }

    public String buildInsert(String schema, String table, List<ColumnSpec> columns, List<List<String>> rows, List<String> dedupKeys) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(schema).append('.').append(table).append('(');
        StringJoiner colJoiner = new StringJoiner(", ");
        for (ColumnSpec c : columns) {
            colJoiner.add(c.getNormalizedName());
        }
        sql.append(colJoiner).append(") VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('(').append(valuesClause(rows.get(i))).append(')');
        }
        if (dedupKeys != null && !dedupKeys.isEmpty()) {
            StringJoiner keyJoin = new StringJoiner(", ");
            for (String k : dedupKeys) {
                keyJoin.add(k);
            }
            sql.append(" ON CONFLICT (").append(keyJoin).append(") DO NOTHING");
        }
        return sql.toString();
    }

    private String valuesClause(List<String> row) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String v : row) {
            joiner.add(formatValue(v));
        }
        return joiner.toString();
    }

    private String formatValue(String v) {
        if (v == null || v.isEmpty()) {
            return "NULL";
        }
        String escaped = v.replace("\\", "\\\\").replace("'", "''");
        return "'" + escaped + "'";
    }
}
