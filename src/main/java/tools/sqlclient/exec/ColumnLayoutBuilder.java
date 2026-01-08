package tools.sqlclient.exec;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 根据投影解析结果、元数据与后端返回的键集合，生成有序且可重复的列定义与行值。
 */
public class ColumnLayoutBuilder {

    public record ColumnLayout(List<ColumnDef> columnDefs, List<String> displayColumns, List<List<String>> rows) {}

    private final Map<String, List<String>> metadataColumns;

    public ColumnLayoutBuilder(Map<String, List<String>> metadataColumns) {
        Map<String, List<String>> normalized = new HashMap<>();
        if (metadataColumns != null) {
            metadataColumns.forEach((k, v) -> normalized.put(normalize(k), v == null ? List.of() : new ArrayList<>(v)));
        }
        this.metadataColumns = normalized;
    }

    public ColumnLayout build(SelectProjectionParser.Projection projection,
                              List<String> providedColumns,
                              List<Map<String, String>> rowMaps) {
        List<ColumnDef> defs = new ArrayList<>();
        List<String> safeProvided = providedColumns == null ? List.of() : new ArrayList<>(providedColumns);
        List<Map<String, String>> safeRows = rowMaps == null ? List.of() : rowMaps;
        int seq = 1;

        if (projection != null && projection.items() != null && !projection.items().isEmpty()) {
            int consumed = 0;
            List<SelectProjectionParser.ProjectionItem> items = projection.items();
            for (int idx = 0; idx < items.size(); idx++) {
                SelectProjectionParser.ProjectionItem item = items.get(idx);
                int remainingExplicit = countRemainingExplicit(items, idx + 1);
                if (item.type() == SelectProjectionParser.ProjectionItem.Type.STAR) {
                    List<String> expanded = resolveStarColumns(item, projection, safeProvided, safeRows, consumed, remainingExplicit);
                    for (String col : expanded) {
                        String display = Objects.requireNonNullElse(col, "");
                        defs.add(new ColumnDef(buildId(display, seq++), display, col));
                    }
                    consumed += expanded.size();
                } else {
                    String display = item.alias() != null ? item.alias() : item.displayLabel();
                    String sourceKey = (consumed < safeProvided.size() && safeProvided.get(consumed) != null)
                            ? safeProvided.get(consumed)
                            : display;
                    defs.add(new ColumnDef(buildId(display, seq++), Objects.requireNonNullElse(display, ""), sourceKey));
                    consumed++;
                }
            }
        }

        if (defs.isEmpty()) {
            List<String> fallback = fallbackColumns(safeProvided, safeRows);
            for (String name : fallback) {
                String display = Objects.requireNonNullElse(name, "");
                defs.add(new ColumnDef(buildId(display, seq++), display, display));
            }
        }

        List<List<String>> rows = materializeRows(defs, safeRows);
        List<String> displayColumns = defs.stream().map(ColumnDef::getDisplayName).collect(Collectors.toList());
        return new ColumnLayout(defs, displayColumns, rows);
    }

    private List<List<String>> materializeRows(List<ColumnDef> defs, List<Map<String, String>> rowMaps) {
        List<List<String>> rows = new ArrayList<>();
        if (defs == null || defs.isEmpty() || rowMaps == null) {
            return rows;
        }
        for (Map<String, String> map : rowMaps) {
            List<String> row = new ArrayList<>(defs.size());
            for (int i = 0; i < defs.size(); i++) {
                Object value = RowValueResolver.resolveValue(defs, map, i);
                row.add(value != null ? value.toString() : null);
            }
            rows.add(row);
        }
        return rows;
    }

    private List<String> resolveStarColumns(SelectProjectionParser.ProjectionItem starItem,
                                            SelectProjectionParser.Projection projection,
                                            List<String> providedColumns,
                                            List<Map<String, String>> rowMaps,
                                            int consumedProvided,
                                            int reserveExplicit) {
        List<String> metadata = findMetadataForAlias(starItem.tableAlias(), projection);
        if (!metadata.isEmpty()) {
            return metadata;
        }
        if (!providedColumns.isEmpty()) {
            if (onlyStar(projection)) {
                return providedColumns;
            }
            int end = Math.max(consumedProvided, providedColumns.size() - reserveExplicit);
            end = Math.min(end, providedColumns.size());
            if (consumedProvided < end) {
                return new ArrayList<>(providedColumns.subList(consumedProvided, end));
            }
        }
        List<String> derived = deriveColumnsFromRows(rowMaps);
        if (!derived.isEmpty()) {
            return derived;
        }
        return List.of(starItem.tableAlias() != null ? starItem.tableAlias() + ".*" : "*");
    }

    private boolean onlyStar(SelectProjectionParser.Projection projection) {
        if (projection == null || projection.items() == null || projection.items().isEmpty()) {
            return false;
        }
        return projection.items().stream().allMatch(it -> it.type() == SelectProjectionParser.ProjectionItem.Type.STAR);
    }

    private int countRemainingExplicit(List<SelectProjectionParser.ProjectionItem> items, int startIndex) {
        int count = 0;
        for (int i = startIndex; i < items.size(); i++) {
            if (items.get(i).type() != SelectProjectionParser.ProjectionItem.Type.STAR) {
                count++;
            }
        }
        return count;
    }

    private List<String> deriveColumnsFromRows(List<Map<String, String>> rows) {
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        if (rows != null) {
            for (Map<String, String> map : rows) {
                if (map != null) {
                    keys.addAll(map.keySet());
                }
            }
        }
        return new ArrayList<>(keys);
    }

    private List<String> fallbackColumns(List<String> providedColumns, List<Map<String, String>> rowMaps) {
        if (providedColumns != null && !providedColumns.isEmpty()) {
            return providedColumns;
        }
        List<String> derived = deriveColumnsFromRows(rowMaps);
        if (!derived.isEmpty()) {
            return derived;
        }
        return List.of("结果列");
    }

    private List<String> findMetadataForAlias(String alias, SelectProjectionParser.Projection projection) {
        if (alias != null) {
            List<String> byAlias = metadataColumns.get(normalize(alias));
            if (byAlias != null && !byAlias.isEmpty()) {
                return byAlias;
            }
            if (projection != null && projection.tableAliases() != null) {
                String table = projection.tableAliases().get(alias);
                if (table != null) {
                    List<String> byTable = metadataColumns.get(normalize(table));
                    if (byTable != null && !byTable.isEmpty()) {
                        return byTable;
                    }
                }
            }
        }
        if (projection != null && projection.tableAliases() != null && projection.tableAliases().size() == 1) {
            String onlyAlias = projection.tableAliases().keySet().iterator().next();
            List<String> sole = metadataColumns.get(normalize(onlyAlias));
            if (sole != null && !sole.isEmpty()) {
                return sole;
            }
            String table = projection.tableAliases().get(onlyAlias);
            if (table != null) {
                List<String> byTable = metadataColumns.get(normalize(table));
                if (byTable != null && !byTable.isEmpty()) {
                    return byTable;
                }
            }
        }
        if (projection != null && projection.tableForStar() != null) {
            List<String> simple = metadataColumns.get(normalize(projection.tableForStar()));
            if (simple != null && !simple.isEmpty()) {
                return simple;
            }
        }
        return List.of();
    }

    private String buildId(String base, int index) {
        String prefix = (base == null || base.isBlank()) ? "col" : base;
        return prefix + "#" + index;
    }

    private String normalize(String name) {
        return name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
    }
}
