package tools.sqlclient.exec;

import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.util.OperationLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 结果列顺序决策器：按 SQL 语义或元数据重排列名与行值。
 */
public class ColumnOrderDecider {
    private final MetadataService metadataService;

    public ColumnOrderDecider(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    public SqlExecResult reorder(SqlExecResult original, Consumer<SqlExecResult> onMetadataReloaded) {
        if (original == null) {
            return null;
        }
        List<String> originalColumns = safeColumns(original.getColumns());
        List<List<String>> originalRows = safeRows(original.getRows());
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(original.getSql());

        List<String> metadataColumns = List.of();
        if (projection.simpleSelectStar() && projection.tableForStar() != null && metadataService != null) {
            metadataColumns = metadataService.loadColumnsFromCache(projection.tableForStar());
            if (metadataColumns.isEmpty()) {
                boolean fetching = metadataService.ensureColumnsCachedAsync(projection.tableForStar(), () -> {
                    if (onMetadataReloaded != null) {
                        SqlExecResult refreshed = reorder(original, null);
                        onMetadataReloaded.accept(refreshed);
                    }
                });
                if (fetching) {
                    OperationLog.log("正在拉取表 " + projection.tableForStar() + " 的字段顺序，加载完成后自动刷新结果");
                }
            }
        }

        List<String> finalColumns = decideFinalColumns(originalColumns, projection, metadataColumns, originalRows);
        List<List<String>> finalRows = reorderRows(originalRows, originalColumns, finalColumns);
        return cloneWith(original, finalColumns, finalRows);
    }

    private List<String> decideFinalColumns(List<String> originalColumns,
                                            SelectProjectionParser.Projection projection,
                                            List<String> metadataColumns,
                                            List<List<String>> rows) {
        if (projection.simpleSelectStar() && !metadataColumns.isEmpty()) {
            return new ArrayList<>(metadataColumns);
        }
        if (!projection.columns().isEmpty()) {
            return new ArrayList<>(projection.columns());
        }
        if (!originalColumns.isEmpty()) {
            return new ArrayList<>(originalColumns);
        }
        if (!metadataColumns.isEmpty()) {
            return new ArrayList<>(metadataColumns);
        }
        if (!rows.isEmpty()) {
            int max = rows.stream().mapToInt(List::size).max().orElse(0);
            List<String> cols = new ArrayList<>();
            for (int i = 0; i < max; i++) {
                cols.add("col_" + (i + 1));
            }
            return cols;
        }
        return new ArrayList<>(List.of("结果列"));
    }

    private List<List<String>> reorderRows(List<List<String>> rows,
                                           List<String> originalColumns,
                                           List<String> finalColumns) {
        if (finalColumns.isEmpty()) {
            return rows;
        }
        if (originalColumns.equals(finalColumns)) {
            return rows;
        }
        boolean useIndexOnly = originalColumns.isEmpty();
        Map<String, Integer> indexMap = buildIndexMap(originalColumns);
        List<List<String>> reordered = new ArrayList<>();
        for (List<String> row : rows) {
            List<String> newRow = new ArrayList<>(finalColumns.size());
            for (int i = 0; i < finalColumns.size(); i++) {
                if (useIndexOnly) {
                    newRow.add(i < row.size() ? row.get(i) : "");
                } else {
                    String col = finalColumns.get(i);
                    Integer idx = indexMap.get(normalize(col));
                    if (idx != null && idx >= 0 && idx < row.size()) {
                        newRow.add(row.get(idx));
                    } else {
                        newRow.add("");
                    }
                }
            }
            reordered.add(newRow);
        }
        return reordered;
    }

    private Map<String, Integer> buildIndexMap(List<String> columns) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String key = normalize(columns.get(i));
            map.putIfAbsent(key, i);
        }
        return map;
    }

    private String normalize(String name) {
        return name == null ? "" : name.trim().toLowerCase();
    }

    private List<String> safeColumns(List<String> columns) {
        if (columns == null) {
            return new ArrayList<>();
        }
        Set<String> seen = new java.util.LinkedHashSet<>();
        List<String> result = new ArrayList<>();
        for (String col : columns) {
            String val = Objects.requireNonNullElse(col, "");
            if (seen.add(val)) {
                result.add(val);
            }
        }
        return result;
    }

    private List<List<String>> safeRows(List<List<String>> rows) {
        if (rows == null) {
            return new ArrayList<>();
        }
        List<List<String>> result = new ArrayList<>();
        for (List<String> row : rows) {
            result.add(new ArrayList<>(row));
        }
        return result;
    }

    private SqlExecResult cloneWith(SqlExecResult original, List<String> columns, List<List<String>> rows) {
        return new SqlExecResult(
                original.getSql(),
                columns,
                rows,
                original.getRowsCount(),
                original.isSuccess(),
                original.getMessage(),
                original.getJobId(),
                original.getStatus(),
                original.getProgressPercent(),
                original.getElapsedMillis(),
                original.getDurationMillis(),
                original.getRowsAffected(),
                original.getReturnedRowCount(),
                original.getActualRowCount(),
                original.getMaxVisibleRows(),
                original.getMaxTotalRows(),
                original.getHasResultSet(),
                original.getPage(),
                original.getPageSize(),
                original.getHasNext(),
                original.getTruncated(),
                original.getNote()
        );
    }
}
