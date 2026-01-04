package tools.sqlclient.importer;

import java.text.Normalizer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 将字段名转换为符合 PG 规范的形式，去重并避免数字开头。
 */
public final class ColumnNameNormalizer {
    private ColumnNameNormalizer() {
    }

    public static List<String> normalize(List<String> headers) {
        Set<String> seen = new HashSet<>();
        return headers.stream().map(h -> normalizeSingle(h, seen)).toList();
    }

    private static String normalizeSingle(String header, Set<String> seen) {
        String base = header == null ? "" : Normalizer.normalize(header.trim().toLowerCase(), Normalizer.Form.NFKC);
        if (base.isEmpty()) {
            base = "col";
        }
        StringBuilder sb = new StringBuilder();
        char[] chars = base.toCharArray();
        for (char c : chars) {
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        String name = sb.toString();
        if (!name.isEmpty() && Character.isDigit(name.charAt(0))) {
            name = "c_" + name;
        }
        String candidate = name;
        int suffix = 1;
        while (seen.contains(candidate) || candidate.isEmpty()) {
            candidate = name + "_" + suffix;
            suffix++;
        }
        seen.add(candidate);
        return candidate;
    }
}
