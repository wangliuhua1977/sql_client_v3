package tools.sqlclient.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 轻量级 SELECT 子句解析器，用于确定列顺序/别名。
 */
public class SelectProjectionParser {

    public static Projection parse(String sql) {
        if (sql == null) {
            return new Projection(List.of(), false, null);
        }
        String trimmed = sql.trim();
        if (trimmed.isEmpty()) {
            return new Projection(List.of(), false, null);
        }
        int selectIdx = indexOfIgnoreCase(trimmed, "select", 0);
        if (selectIdx < 0) {
            return new Projection(List.of(), false, null);
        }
        int fromIdx = findTopLevelKeyword(trimmed, "from", selectIdx + 6);
        if (fromIdx < 0) {
            return new Projection(List.of(), false, null);
        }
        String projectionPart = trimmed.substring(selectIdx + 6, fromIdx);
        String fromPart = trimmed.substring(fromIdx + 4);
        List<String> columns = parseProjectionList(projectionPart);
        boolean simpleStar = isSimpleSelectStar(columns, fromPart);
        String starTable = simpleStar ? parseSimpleTableName(fromPart) : null;
        return new Projection(columns, simpleStar, starTable);
    }

    private static boolean isSimpleSelectStar(List<String> columns, String fromPart) {
        if (columns.size() != 1) {
            return false;
        }
        String col = columns.get(0);
        if (!"*".equals(col)) {
            return false;
        }
        String tableName = parseSimpleTableName(fromPart);
        return tableName != null && !tableName.isBlank();
    }

    private static String parseSimpleTableName(String fromPart) {
        if (fromPart == null) {
            return null;
        }
        String trimmed = fromPart.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.contains(" join ") || lower.contains(",")) {
            return null;
        }
        int whereIdx = findTopLevelKeyword(trimmed, "where", 0);
        if (whereIdx >= 0) {
            trimmed = trimmed.substring(0, whereIdx);
        }
        int groupIdx = findTopLevelKeyword(trimmed, "group", 0);
        if (groupIdx >= 0) {
            trimmed = trimmed.substring(0, groupIdx);
        }
        int orderIdx = findTopLevelKeyword(trimmed, "order", 0);
        if (orderIdx >= 0) {
            trimmed = trimmed.substring(0, orderIdx);
        }
        int limitIdx = findTopLevelKeyword(trimmed, "limit", 0);
        if (limitIdx >= 0) {
            trimmed = trimmed.substring(0, limitIdx);
        }
        String[] tokens = trimmed.trim().split("\\s+");
        if (tokens.length == 0) {
            return null;
        }
        String table = cleanIdentifier(tokens[0]);
        int dot = table.lastIndexOf('.') + 1;
        if (dot > 0 && dot < table.length()) {
            table = table.substring(dot);
        }
        return table;
    }

    private static List<String> parseProjectionList(String projectionPart) {
        List<String> list = new ArrayList<>();
        if (projectionPart == null) {
            return list;
        }
        List<String> segments = splitTopLevel(projectionPart, ',');
        for (String seg : segments) {
            String trimmed = seg.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String display = extractAlias(trimmed);
            list.add(display);
        }
        return list;
    }

    private static String extractAlias(String expr) {
        String lowered = expr.toLowerCase(Locale.ROOT);
        int asIdx = lowered.lastIndexOf(" as ");
        if (asIdx > 0) {
            String alias = expr.substring(asIdx + 4).trim();
            return cleanIdentifier(alias);
        }
        String[] parts = expr.split("\\s+");
        if (parts.length >= 2) {
            String alias = parts[parts.length - 1].trim();
            if (!alias.isEmpty() && Character.isLetterOrDigit(alias.charAt(0))) {
                return cleanIdentifier(alias);
            }
        }
        return deriveNameFromExpression(expr.trim());
    }

    private static String deriveNameFromExpression(String expr) {
        if (expr == null || expr.isBlank()) {
            return expr;
        }
        String cleaned = expr.trim();
        if (cleaned.endsWith(".*")) {
            cleaned = cleaned.substring(0, cleaned.length() - 2);
        }
        int dot = cleaned.lastIndexOf('.');
        if (dot >= 0 && dot < cleaned.length() - 1) {
            return cleanIdentifier(cleaned.substring(dot + 1));
        }
        return cleanIdentifier(cleaned);
    }

    private static List<String> splitTopLevel(String text, char delimiter) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
            } else if (c == '"' && !inSingle) {
                inDouble = !inDouble;
            } else if (!inSingle && !inDouble) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth = Math.max(0, depth - 1);
                } else if (c == delimiter && depth == 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(c);
        }
        parts.add(current.toString());
        return parts;
    }

    private static int indexOfIgnoreCase(String text, String search, int fromIndex) {
        return text.toLowerCase(Locale.ROOT).indexOf(search.toLowerCase(Locale.ROOT), fromIndex);
    }

    private static int findTopLevelKeyword(String text, String keyword, int fromIndex) {
        String lower = text.toLowerCase(Locale.ROOT);
        int depth = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = fromIndex; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
            } else if (c == '"' && !inSingle) {
                inDouble = !inDouble;
            }
            if (inSingle || inDouble) continue;
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
            }
            if (depth == 0 && lower.regionMatches(true, i, keyword, 0, keyword.length())) {
                return i;
            }
        }
        return -1;
    }

    private static String cleanIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String trimmed = identifier.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || (trimmed.startsWith("`") && trimmed.endsWith("`"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    public record Projection(List<String> columns, boolean simpleSelectStar, String tableForStar) {}
}
