package tools.sqlclient.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 展示列名去重工具：允许同名列存在，但 JTable 表头必须唯一。
 */
public final class ColumnDisplayNameResolver {
    private ColumnDisplayNameResolver() {
    }

    public static List<String> resolveDisplayNames(List<String> columnNames, List<ColumnMeta> metas) {
        List<String> safeNames = columnNames == null ? List.of() : columnNames;
        List<String> result = new ArrayList<>(safeNames.size());
        Map<String, Integer> nameCounts = new HashMap<>();
        for (String name : safeNames) {
            String key = name == null ? "" : name;
            nameCounts.put(key, nameCounts.getOrDefault(key, 0) + 1);
        }

        for (int i = 0; i < safeNames.size(); i++) {
            String name = safeNames.get(i);
            ColumnMeta meta = (metas != null && i < metas.size()) ? metas.get(i) : null;
            String base = resolveBaseDisplayName(name, meta, nameCounts);
            result.add(base);
        }

        return deduplicate(result);
    }

    private static String resolveBaseDisplayName(String name, ColumnMeta meta, Map<String, Integer> nameCounts) {
        String trimmed = name == null ? "" : name.trim();
        String base = meta != null && meta.getDisplayLabel() != null ? meta.getDisplayLabel().trim() : trimmed;
        if (base.isBlank()) {
            base = trimmed;
        }
        if (!base.isBlank() && nameCounts.getOrDefault(trimmed, 0) > 1 && meta != null) {
            String tableName = meta.getTableName();
            if (tableName != null && !tableName.isBlank()) {
                return tableName + "." + trimmed;
            }
            String schemaName = meta.getSchemaName();
            if (schemaName != null && !schemaName.isBlank()) {
                return schemaName + "." + trimmed;
            }
        }
        return base.isBlank() ? "col" : base;
    }

    private static List<String> deduplicate(List<String> names) {
        List<String> result = new ArrayList<>(names.size());
        Map<String, Integer> seen = new HashMap<>();
        for (String name : names) {
            String base = name == null ? "" : name;
            int count = seen.getOrDefault(base, 0) + 1;
            seen.put(base, count);
            if (count > 1) {
                result.add(base + "#" + count);
            } else {
                result.add(base);
            }
        }
        return result;
    }
}
