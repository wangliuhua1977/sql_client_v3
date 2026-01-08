package tools.sqlclient.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 按列索引解析行值，避免依赖列名与返回 key 完全一致。
 */
public final class RowValueResolver {
    private static final Logger log = LoggerFactory.getLogger(RowValueResolver.class);
    private static final AtomicBoolean loggedCaseInsensitive = new AtomicBoolean(false);
    private static final AtomicBoolean loggedOrderedFallback = new AtomicBoolean(false);
    private static final AtomicBoolean loggedSingleFallback = new AtomicBoolean(false);

    private RowValueResolver() {
    }

    public static Object resolveValue(List<ColumnDef> columns, Object row, int columnIndex) {
        if (row == null || columns == null || columnIndex < 0 || columnIndex >= columns.size()) {
            return null;
        }
        if (row instanceof List<?> list) {
            return columnIndex < list.size() ? list.get(columnIndex) : null;
        }
        if (row instanceof Object[] array) {
            return columnIndex < array.length ? array[columnIndex] : null;
        }
        if (row instanceof Map<?, ?> map) {
            ColumnDef def = columns.get(columnIndex);
            String dataKey = def != null ? def.getSourceKey() : null;
            String displayLabel = def != null ? def.getDisplayName() : null;
            return resolveFromMap(castStringMap(map), dataKey, displayLabel, columns.size(), columnIndex);
        }
        return null;
    }

    public static Object resolveValue(List<String> columns, Object row, int columnIndex) {
        if (row == null || columns == null || columnIndex < 0 || columnIndex >= columns.size()) {
            return null;
        }
        if (row instanceof List<?> list) {
            return columnIndex < list.size() ? list.get(columnIndex) : null;
        }
        if (row instanceof Object[] array) {
            return columnIndex < array.length ? array[columnIndex] : null;
        }
        if (row instanceof Map<?, ?> map) {
            String name = columns.get(columnIndex);
            return resolveFromMap(castStringMap(map), name, name, columns.size(), columnIndex);
        }
        return null;
    }

    private static Object resolveFromMap(Map<String, ?> map,
                                         String dataKey,
                                         String displayLabel,
                                         int columnCount,
                                         int columnIndex) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        ResolvedValue resolved = findValue(map, dataKey, false);
        if (resolved.found()) {
            return resolved.value();
        }
        resolved = findValue(map, dataKey, true);
        if (resolved.found()) {
            logFallbackOnce(loggedCaseInsensitive, "case-insensitive key match", dataKey, displayLabel, map);
            return resolved.value();
        }
        if (displayLabel != null && !displayLabel.equals(dataKey)) {
            resolved = findValue(map, displayLabel, false);
            if (resolved.found()) {
                return resolved.value();
            }
            resolved = findValue(map, displayLabel, true);
            if (resolved.found()) {
                logFallbackOnce(loggedCaseInsensitive, "case-insensitive label match", dataKey, displayLabel, map);
                return resolved.value();
            }
        }
        if (isOrderedMap(map) && map.size() == columnCount) {
            Object value = valueByOrder(map, columnIndex);
            logFallbackOnce(loggedOrderedFallback, "ordered map fallback", dataKey, displayLabel, map);
            return value;
        }
        if (columnCount == 1 && map.size() == 1) {
            logFallbackOnce(loggedSingleFallback, "single-column fallback", dataKey, displayLabel, map);
            return map.values().iterator().next();
        }
        return null;
    }

    private static ResolvedValue findValue(Map<String, ?> map, String key, boolean ignoreCase) {
        if (key == null || key.isBlank()) {
            return ResolvedValue.NOT_FOUND;
        }
        if (!ignoreCase) {
            if (map.containsKey(key)) {
                return new ResolvedValue(true, map.get(key));
            }
            return ResolvedValue.NOT_FOUND;
        }
        String target = normalize(key);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (target.equals(normalize(entry.getKey()))) {
                return new ResolvedValue(true, entry.getValue());
            }
        }
        return ResolvedValue.NOT_FOUND;
    }

    private static Object valueByOrder(Map<String, ?> map, int index) {
        if (index < 0) {
            return null;
        }
        int i = 0;
        for (Object value : map.values()) {
            if (i == index) {
                return value;
            }
            i++;
        }
        return null;
    }

    private static boolean isOrderedMap(Map<String, ?> map) {
        if (map instanceof java.util.LinkedHashMap) {
            return true;
        }
        return map.getClass().getName().equals("com.google.gson.internal.LinkedTreeMap");
    }

    private static Map<String, ?> castStringMap(Map<?, ?> map) {
        @SuppressWarnings("unchecked")
        Map<String, ?> typed = (Map<String, ?>) map;
        return typed;
    }

    private static void logFallbackOnce(AtomicBoolean flag,
                                        String reason,
                                        String dataKey,
                                        String displayLabel,
                                        Map<String, ?> map) {
        if (flag.compareAndSet(false, true)) {
            log.debug("Row value fallback used: reason={}, dataKey={}, displayLabel={}, rowKeys={}",
                    reason, dataKey, displayLabel, map.keySet());
        }
    }

    private static String normalize(String key) {
        return key == null ? "" : key.trim().toLowerCase(Locale.ROOT);
    }

    private record ResolvedValue(boolean found, Object value) {
        static final ResolvedValue NOT_FOUND = new ResolvedValue(false, null);
    }
}
