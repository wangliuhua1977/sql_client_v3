package tools.sqlclient.pg;

import tools.sqlclient.exec.SqlExecResult;
import tools.sqlclient.exec.SqlExecutionService;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.ThreadPools;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 通过现有异步 SQL 链路访问 PostgreSQL 函数/存储过程元信息与源码。
 */
public class PgRoutineService {
    private static final String LIST_SQL = "SELECT\n" +
            "  p.oid::bigint AS oid,\n" +
            "  n.nspname AS schema_name,\n" +
            "  p.proname AS object_name,\n" +
            "  p.prokind AS object_kind,\n" +
            "  pg_get_function_identity_arguments(p.oid) AS identity_args,\n" +
            "  pg_get_function_arguments(p.oid) AS full_args,\n" +
            "  pg_get_function_result(p.oid) AS result_type\n" +
            "FROM pg_proc p\n" +
            "JOIN pg_namespace n ON n.oid = p.pronamespace\n" +
            "WHERE n.nspname = 'leshan'\n" +
            "  AND p.prokind IN ('f','p')";

    private final SqlExecutionService sqlExecutionService;

    public PgRoutineService(SqlExecutionService sqlExecutionService) {
        this.sqlExecutionService = sqlExecutionService;
    }

    public CompletableFuture<List<RoutineInfo>> listLeshanRoutinesByName(String name) {
        return CompletableFuture.supplyAsync(() -> {
            StringBuilder sql = new StringBuilder(LIST_SQL);
            if (name != null && !name.isBlank()) {
                sql.append(" AND p.proname = '").append(escapeLiteral(name.trim())).append("'");
            }
            sql.append(" ORDER BY p.proname, identity_args");
            SqlExecResult result = sqlExecutionService.executeSyncAllPagesWithDbUser(sql.toString(), "leshan");
            return toRoutines(result);
        }, ThreadPools.NETWORK_POOL);
    }

    public CompletableFuture<RoutineInfo> getRoutineInfo(long oid) {
        return CompletableFuture.supplyAsync(() -> {
            String sql = LIST_SQL + " AND p.oid = " + oid + " ORDER BY identity_args";
            SqlExecResult result = sqlExecutionService.executeSyncAllPagesWithDbUser(sql, "leshan");
            List<RoutineInfo> list = toRoutines(result);
            return list.isEmpty() ? null : list.get(0);
        }, ThreadPools.NETWORK_POOL);
    }

    public CompletableFuture<String> loadRoutineDdl(long oid) {
        return CompletableFuture.supplyAsync(() -> {
            String sql = "SELECT pg_get_functiondef(" + oid + ") AS ddl;";
            SqlExecResult result = sqlExecutionService.executeSyncAllPagesWithDbUser(sql, "leshan");
            if (result.getRowMaps() != null && !result.getRowMaps().isEmpty()) {
                Map<String, String> row = result.getRowMaps().get(0);
                return row.getOrDefault("ddl", "");
            }
            if (result.getRows() != null && !result.getRows().isEmpty() && !result.getRows().get(0).isEmpty()) {
                return result.getRows().get(0).get(0);
            }
            return "";
        }, ThreadPools.NETWORK_POOL);
    }

    public CompletableFuture<SqlExecResult> publishRoutineDdl(String ddl) {
        return CompletableFuture.supplyAsync(() -> sqlExecutionService.executeSyncAllPagesWithDbUser(ddl, "leshan"),
                ThreadPools.NETWORK_POOL);
    }

    public CompletableFuture<SqlExecResult> runRoutine(long oid, String generatedSql) {
        OperationLog.log("调试运行 routine oid=" + oid + " sql=" + OperationLog.abbreviate(generatedSql, 120));
        return CompletableFuture.supplyAsync(() -> sqlExecutionService.executeSyncAllPagesWithDbUser(generatedSql, "leshan"),
                ThreadPools.NETWORK_POOL);
    }

    private List<RoutineInfo> toRoutines(SqlExecResult result) {
        List<RoutineInfo> list = new ArrayList<>();
        if (result == null || result.getRowMaps() == null) {
            return list;
        }
        for (Map<String, String> row : result.getRowMaps()) {
            try {
                long oid = Long.parseLong(String.valueOf(row.get("oid")));
                list.add(new RoutineInfo(
                        oid,
                        row.getOrDefault("schema_name", "leshan"),
                        row.getOrDefault("object_name", ""),
                        row.getOrDefault("object_kind", ""),
                        row.getOrDefault("identity_args", ""),
                        row.getOrDefault("full_args", ""),
                        row.getOrDefault("result_type", ""))
                );
            } catch (Exception ignored) {
                // 忽略异常行，避免 UI 崩溃
            }
        }
        return list;
    }

    private String escapeLiteral(String raw) {
        return raw.replace("'", "''");
    }
}
