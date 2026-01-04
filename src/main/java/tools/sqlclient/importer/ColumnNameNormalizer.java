package tools.sqlclient.importer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ColumnNameNormalizer {
    public List<String> normalize(List<String> headers) {
        List<String> result = new ArrayList<>(headers.size());
        Map<String, Integer> duplicates = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            String raw = headers.get(i) == null ? "" : headers.get(i).trim();
            if (raw.isEmpty()) {
                raw = "col_" + (i + 1);
            }
            String normalized = raw.toLowerCase(Locale.ROOT);
            normalized = normalized.replaceAll("[^a-z0-9_]", "_");
            normalized = normalized.replaceAll("_+", "_");
            normalized = normalized.replaceAll("^_+|_+$", "");
            if (normalized.isEmpty()) {
                normalized = "col_" + (i + 1);
            }
            if (Character.isDigit(normalized.charAt(0))) {
                normalized = "c_" + normalized;
            }
            int count = duplicates.getOrDefault(normalized, 0) + 1;
            duplicates.put(normalized, count);
            if (count > 1) {
                normalized = normalized + "_" + count;
            }
            result.add(normalized);
        }
        return result;
    }
}
