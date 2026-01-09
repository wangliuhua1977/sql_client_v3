package tools.sqlclient.exec;

import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.util.OperationLog;

import java.util.List;
import java.util.function.Consumer;
import java.util.*;

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
        if (!shouldReorder(original)) {
            return original;
        }
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(original.getSql());

        Map<String, List<String>> metadataColumns = loadMetadataForStars(projection, original, onMetadataReloaded);
        List<Map<String, String>> rowMaps = safeRowMaps(original);
        ColumnLayoutBuilder builder = new ColumnLayoutBuilder(metadataColumns);
        ColumnLayoutBuilder.ColumnLayout layout = builder.build(projection, original.getColumns(), rowMaps);

        return cloneWith(original, layout.displayColumns(), layout.columnDefs(), layout.rows(), rowMaps);
    }

    private boolean shouldReorder(SqlExecResult original) {
        Boolean hasResultSet = original.getHasResultSet();
        if (Boolean.FALSE.equals(hasResultSet)) {
            return false;
        }
        if (!Boolean.TRUE.equals(hasResultSet)
                && SqlTopLevelClassifier.classify(original.getSql()) != SqlTopLevelClassifier.TopLevelType.RESULT_SET) {
            return false;
        }
        List<String> columns = original.getColumns();
        if (columns == null || columns.isEmpty()) {
            return false;
        }
        if (hasDuplicateNames(columns)) {
            return false;
        }
        if (original.getColumnDefs() != null && !original.getColumnDefs().isEmpty()
                && original.getColumnDefs().size() != columns.size()) {
            return false;
        }
        List<List<String>> rows = original.getRows();
        if (rows != null) {
            for (List<String> row : rows) {
                if (row != null && row.size() != columns.size()) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean hasDuplicateNames(List<String> columns) {
        if (columns == null) {
            return false;
        }
        Set<String> seen = new HashSet<>();
        for (String name : columns) {
            String key = name == null ? "" : name;
            if (!seen.add(key)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, List<String>> loadMetadataForStars(SelectProjectionParser.Projection projection,
                                                           SqlExecResult original,
                                                           Consumer<SqlExecResult> onMetadataReloaded) {
        Map<String, List<String>> meta = new HashMap<>();
        if (metadataService == null || projection == null || projection.items() == null) {
            return meta;
        }
        Set<String> requested = new HashSet<>();
        for (SelectProjectionParser.ProjectionItem item : projection.items()) {
            if (item.type() != SelectProjectionParser.ProjectionItem.Type.STAR) continue;
            String table = resolveTableName(item, projection);
            if (table == null || table.isBlank()) {
                continue;
            }
            List<String> cached = metadataService.loadColumnsFromCache(table);
            if (cached == null) {
                cached = List.of();
            }
            String key = item.tableAlias() != null ? item.tableAlias() : table;
            meta.put(key, cached);
            if (cached.isEmpty() && requested.add(table.toLowerCase())) {
                boolean fetching = metadataService.ensureColumnsCachedAsync(table, () -> {
                    if (onMetadataReloaded != null) {
                        SqlExecResult refreshed = reorder(original, null);
                        onMetadataReloaded.accept(refreshed);
                    }
                });
                if (fetching) {
                    OperationLog.log("正在拉取表 " + table + " 的字段顺序，加载完成后自动刷新结果");
                }
            }
        }
        return meta;
    }

    private String resolveTableName(SelectProjectionParser.ProjectionItem item, SelectProjectionParser.Projection projection) {
        if (item.tableAlias() != null && projection.tableAliases() != null) {
            String mapped = projection.tableAliases().get(item.tableAlias());
            if (mapped != null) {
                return mapped;
            }
        }
        if (projection.tableForStar() != null) {
            return projection.tableForStar();
        }
        if (projection.tableAliases() != null && projection.tableAliases().size() == 1) {
            return projection.tableAliases().values().iterator().next();
        }
        return item.tableAlias();
    }

    private List<Map<String, String>> safeRowMaps(SqlExecResult original) {
        if (original.getRowMaps() != null && !original.getRowMaps().isEmpty()) {
            List<Map<String, String>> copy = new ArrayList<>();
            for (Map<String, String> map : original.getRowMaps()) {
                copy.add(new LinkedHashMap<>(map));
            }
            return copy;
        }
        List<Map<String, String>> result = new ArrayList<>();
        List<String> cols = original.getColumns() != null ? original.getColumns() : List.of();
        if (original.getRows() != null) {
            for (List<String> row : original.getRows()) {
                Map<String, String> map = new LinkedHashMap<>();
                for (int i = 0; i < cols.size(); i++) {
                    map.put(cols.get(i), i < row.size() ? row.get(i) : null);
                }
                result.add(map);
            }
        }
        return result;
    }

    private SqlExecResult cloneWith(SqlExecResult original, List<String> columns, List<ColumnDef> columnDefs,
                                    List<List<String>> rows, List<Map<String, String>> rowMaps) {
        return SqlExecResult.builder(original.getSql())
                .columns(columns)
                .columnDefs(columnDefs)
                .rows(rows)
                .rowMaps(rowMaps)
                .rowCount(rows != null ? rows.size() : original.getRowsCount())
                .success(original.isSuccess())
                .message(original.getMessage())
                .jobId(original.getJobId())
                .status(original.getStatus())
                .progressPercent(original.getProgressPercent())
                .elapsedMillis(original.getElapsedMillis())
                .durationMillis(original.getDurationMillis())
                .rowsAffected(original.getRowsAffected())
                .returnedRowCount(original.getReturnedRowCount())
                .actualRowCount(original.getActualRowCount())
                .maxVisibleRows(original.getMaxVisibleRows())
                .maxTotalRows(original.getMaxTotalRows())
                .totalRows(original.getTotalRows())
                .hasResultSet(original.getHasResultSet())
                .page(original.getPage())
                .pageSize(original.getPageSize())
                .hasNext(original.getHasNext())
                .truncated(original.getTruncated())
                .note(original.getNote())
                .queuedAt(original.getQueuedAt())
                .queueDelayMillis(original.getQueueDelayMillis())
                .overloaded(original.getOverloaded())
                .threadPool(original.getThreadPool())
                .commandTag(original.getCommandTag())
                .updateCount(original.getUpdateCount())
                .notices(original.getNotices())
                .warnings(original.getWarnings())
                .build();
    }
}
