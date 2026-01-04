package tools.sqlclient.importer;

import java.util.HashSet;
import java.util.Set;

public class ColumnNameNormalizer {
    public static String[] normalize(String[] headers) {
        Set<String> used = new HashSet<>();
        String[] result = new String[headers.length];
        for (int i = 0; i < headers.length; i++) {
            String name = headers[i] == null ? "col" + (i + 1) : headers[i].trim().toLowerCase();
            if (name.isEmpty()) {
                name = "col" + (i + 1);
            }
            name = name.replaceAll("[^a-z0-9_]+", "_");
            if (name.matches("^[0-9].*")) {
                name = "c_" + name;
            }
            String base = name;
            int suffix = 1;
            while (used.contains(name)) {
                name = base + "_" + suffix++;
            }
            used.add(name);
            result[i] = name;
        }
        return result;
    }
}
