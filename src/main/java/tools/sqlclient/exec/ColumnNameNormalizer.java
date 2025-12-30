package tools.sqlclient.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 提供列名自动修复与归一化能力，保障结果集列名非空且稳定。
 */
public final class ColumnNameNormalizer {

    private static final Pattern SIMPLE_AGG_PATTERN = Pattern.compile("^(?<func>sum|avg|min|max|count)\\s*\\(\\s*(?<arg>[^)]*)\\s*\\)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SIMPLE_IDENTIFIER = Pattern.compile("^[A-Za-z_][A-Za-z0-9_\\.]*$");

    private ColumnNameNormalizer() {
    }

    public static List<String> normalize(List<String> rawNames, List<String> expressions) {
        List<String> safeNames = rawNames == null ? List.of() : rawNames;
        List<String> exprs = expressions == null ? List.of() : expressions;
        int size = Math.max(safeNames.size(), exprs.size());
        List<String> result = new ArrayList<>(size);
        int complexSumCount = 0;

        for (int i = 0; i < size; i++) {
            String provided = i < safeNames.size() ? clean(safeNames.get(i)) : null;
            String expr = i < exprs.size() ? clean(exprs.get(i)) : null;

            String candidate = provided;
            if (candidate == null || candidate.isBlank()) {
                candidate = deriveFromExpression(expr);
                if (candidate == null && expr != null && isComplexSum(expr)) {
                    complexSumCount++;
                    candidate = complexSumCount == 1 ? "sum_" : "sum_" + complexSumCount;
                }
            }

            if ((candidate == null || candidate.isBlank()) && expr != null && isComplexSum(expr)) {
                complexSumCount++;
                candidate = complexSumCount == 1 ? "sum_" : "sum_" + complexSumCount;
            }

            if (candidate == null || candidate.isBlank()) {
                candidate = fallbackName(i);
            }

            result.add(candidate);
        }

        return deduplicate(result);
    }

    private static String deriveFromExpression(String expr) {
        if (expr == null || expr.isBlank()) {
            return null;
        }
        String trimmed = expr.trim();
        if (isComplexSum(trimmed)) {
            return null;
        }
        Matcher m = SIMPLE_AGG_PATTERN.matcher(trimmed);
        if (m.find()) {
            String func = m.group("func").toLowerCase(Locale.ROOT);
            String arg = m.group("arg");
            if ("count".equals(func)) {
                if ("*".equals(arg.trim())) {
                    return "count_all";
                }
                if (arg.trim().matches("^[0-9]+$")) {
                    return "count_" + arg.trim();
                }
            }
            String normalizedArg = arg.replaceAll("[^A-Za-z0-9]+", "_").replaceAll("_+", "_");
            normalizedArg = normalizedArg.replaceAll("^_+|_+$", "");
            if (normalizedArg.isBlank()) {
                normalizedArg = "value";
            }
            return func + "_" + normalizedArg.toLowerCase(Locale.ROOT);
        }
        return sanitizeExpression(trimmed);
    }

    private static boolean isComplexSum(String expr) {
        if (expr == null) {
            return false;
        }
        String trimmed = expr.trim();
        if (!trimmed.toLowerCase(Locale.ROOT).startsWith("sum(")) {
            return false;
        }
        int open = trimmed.indexOf('(');
        int close = trimmed.lastIndexOf(')');
        if (close <= open) {
            return false;
        }
        String inner = trimmed.substring(open + 1, close).trim();
        if (inner.isEmpty()) {
            return false;
        }
        if (!SIMPLE_IDENTIFIER.matcher(inner).matches()) {
            return true;
        }
        String lower = inner.toLowerCase(Locale.ROOT);
        return lower.contains("case") || lower.contains("when") || lower.contains("then")
                || lower.contains("else") || lower.contains("end") || lower.contains("distinct")
                || lower.contains("coalesce") || inner.contains(",") || inner.contains("(")
                || inner.contains(")") || inner.matches(".*[\\+\\-\\*/].*") || inner.contains(" ");
    }

    private static String sanitizeExpression(String expr) {
        if (expr == null || expr.isBlank()) {
            return null;
        }
        String lowered = expr.toLowerCase(Locale.ROOT);
        String sanitized = lowered.replaceAll("[^a-z0-9]+", "_").replaceAll("_+", "_");
        sanitized = sanitized.replaceAll("^_+|_+$", "");
        if (sanitized.isBlank()) {
            sanitized = "expr";
        }
        if (!Character.isLetter(sanitized.charAt(0))) {
            sanitized = "expr_" + sanitized;
        }
        return sanitized;
    }

    private static String fallbackName(int index) {
        return "col_" + (index + 1);
    }

    private static List<String> deduplicate(List<String> names) {
        List<String> result = new ArrayList<>(names.size());
        java.util.Map<String, Integer> seen = new java.util.HashMap<>();
        for (String name : names) {
            String base = name == null ? "" : name;
            int count = seen.getOrDefault(base, 0) + 1;
            seen.put(base, count);
            if (count > 1) {
                result.add(base + "_" + count);
            } else {
                result.add(base);
            }
        }
        return result;
    }

    private static String clean(String text) {
        return text == null ? null : text.trim();
    }
}
