package tools.sqlclient.importer;

import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionService;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PasteImportService {
    private final SqlExecutionService sqlExecutionService;

    public PasteImportService(SqlExecutionService sqlExecutionService) {
        this.sqlExecutionService = sqlExecutionService;
    }

    public CompletableFuture<SqlExecResult> createAndInsert(String schema, String table, List<ColumnSpec> columns, List<List<String>> rows, boolean emptyAsNull) {
        PgSqlBuilder builder = new PgSqlBuilder();
        String ddl = builder.buildCreateTableSql(schema, table, columns);
        List<String> inserts = builder.buildInsertBatches(schema, table, columns, rows, emptyAsNull, 300);
        StringBuilder sql = new StringBuilder();
        sql.append(ddl).append("\nBEGIN;\n");
        for (String i : inserts) {
            sql.append(i).append('\n');
        }
        sql.append("COMMIT;");
        return execute(sql.toString());
    }

    public CompletableFuture<SqlExecResult> truncateAndInsert(String schema, String table, List<ColumnSpec> columns, List<List<String>> rows, boolean emptyAsNull) {
        PgSqlBuilder builder = new PgSqlBuilder();
        List<String> inserts = builder.buildInsertBatches(schema, table, columns, rows, emptyAsNull, 300);
        StringBuilder sql = new StringBuilder();
        sql.append("BEGIN; TRUNCATE TABLE ").append(schema).append('.').append(table).append(";\n");
        for (String i : inserts) {
            sql.append(i).append('\n');
        }
        sql.append("COMMIT;");
        return execute(sql.toString());
    }

    public CompletableFuture<SqlExecResult> appendInsert(String schema, String table, List<ColumnSpec> columns, List<List<String>> rows, boolean emptyAsNull) {
        PgSqlBuilder builder = new PgSqlBuilder();
        List<String> inserts = builder.buildInsertBatches(schema, table, columns, rows, emptyAsNull, 300);
        StringBuilder sql = new StringBuilder();
        sql.append("BEGIN;\n");
        for (String i : inserts) {
            sql.append(i).append('\n');
        }
        sql.append("COMMIT;");
        return execute(sql.toString());
    }

    private CompletableFuture<SqlExecResult> execute(String sql) {
        CompletableFuture<SqlExecResult> future = new CompletableFuture<>();
        sqlExecutionService.execute(sql, future::complete, ex -> future.completeExceptionally(ex));
        return future;
    }
}
