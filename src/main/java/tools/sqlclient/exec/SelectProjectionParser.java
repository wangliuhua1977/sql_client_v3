package tools.sqlclient.exec;

import java.util.*;

/**
 * 轻量级 SELECT 子句解析器，用于确定列顺序/别名。
 */
public class SelectProjectionParser {

    public static Projection parse(String sql) {
        if (sql == null) {
            return new Projection(List.of(), false, Map.of(), null);
        }
        String trimmed = sql.trim();
        if (trimmed.isEmpty()) {
            return new Projection(List.of(), false, Map.of(), null);
        }
        int selectIdx = indexOfIgnoreCase(trimmed, "select", 0);
        if (selectIdx < 0) {
            return new Projection(List.of(), false, Map.of(), null);
        }
        int fromIdx = findTopLevelKeyword(trimmed, "from", selectIdx + 6);
        if (fromIdx < 0) {
            return new Projection(List.of(), false, Map.of(), null);
        }
        String projectionPart = trimmed.substring(selectIdx + 6, fromIdx);
        String fromPart = trimmed.substring(fromIdx + 4);
        boolean distinct = projectionPart.trim().toLowerCase(Locale.ROOT).startsWith("distinct");
        if (distinct) {
            projectionPart = projectionPart.trim().substring("distinct".length()).trim();
        }
        List<ProjectionItem> items = parseProjectionList(projectionPart);
        Map<String, String> tableAliases = parseTableAliases(fromPart);
        String primaryTable = parseSimpleTableName(fromPart);
        return new Projection(items, distinct, tableAliases, primaryTable);
    }

    private static List<ProjectionItem> parseProjectionList(String projectionPart) {
        List<ProjectionItem> list = new ArrayList<>();
        if (projectionPart == null) {
            return list;
        }
        List<String> segments = splitTopLevel(projectionPart, ',');
        for (String seg : segments) {
            String trimmed = seg.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            ProjectionItem.Type type = isStar(trimmed) ? ProjectionItem.Type.STAR : ProjectionItem.Type.EXPR;
            if (type == ProjectionItem.Type.STAR) {
                String alias = parseStarAlias(trimmed);
                list.add(new ProjectionItem(type, alias, trimmed, null, "*"));
            } else {
                String alias = extractAlias(trimmed);
                String display = alias != null ? alias : deriveNameFromExpression(trimmed);
                list.add(new ProjectionItem(type, null, trimmed, alias, display));
            }
        }
        return list;
    }

    private static String parseStarAlias(String expr) {
        String cleaned = expr.trim();
        if ("*".equals(cleaned)) {
            return null;
        }
        if (cleaned.endsWith(".*")) {
            String maybeAlias = cleaned.substring(0, cleaned.length() - 2).trim();
            return maybeAlias.isEmpty() ? null : cleanIdentifier(maybeAlias);
        }
        return null;
    }

    private static boolean isStar(String expr) {
        String cleaned = expr.trim();
        if ("*".equals(cleaned)) {
            return true;
        }
        return cleaned.endsWith(".*") && !cleaned.contains(" ");
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

    private static Map<String, String> parseTableAliases(String fromPart) {
        Map<String, String> map = new LinkedHashMap<>();
        if (fromPart == null) {
            return map;
        }
        String trimmed = stripAfterClauses(fromPart);
        List<String> segments = splitTopLevel(trimmed, ',');
        for (String seg : segments) {
            String cleaned = stripAfterJoin(seg.trim());
            if (cleaned.isEmpty()) continue;
            String[] tokens = cleaned.split("\\s+");
            if (tokens.length == 0) continue;
            String table = cleanIdentifier(tokens[0]);
            if (table == null || table.isBlank()) continue;
            String alias = tokens.length >= 2 ? cleanIdentifier(tokens[1]) : table;
            if (alias != null && !alias.isBlank()) {
                map.put(alias, table);
            }
            map.putIfAbsent(table, table);
        }
        return map;
    }

    private static String stripAfterJoin(String text) {
        int joinIdx = findTopLevelKeyword(text, "join", 0);
        if (joinIdx > 0) {
            return text.substring(0, joinIdx);
        }
        return text;
    }

    private static String stripAfterClauses(String fromPart) {
        String trimmed = fromPart.trim();
        for (String keyword : List.of("where", "group", "order", "limit", "having", "offset")) {
            int idx = findTopLevelKeyword(trimmed, keyword, 0);
            if (idx >= 0) {
                trimmed = trimmed.substring(0, idx);
            }
        }
        return trimmed;
    }

    private static String parseSimpleTableName(String fromPart) {
        Map<String, String> aliases = parseTableAliases(fromPart);
        if (!aliases.isEmpty()) {
            java.util.LinkedHashSet<String> distinct = new java.util.LinkedHashSet<>(aliases.values());
            if (distinct.size() == 1) {
                return distinct.iterator().next();
            }
        }
        return null;
    }

    private static String extractAlias(String expr) {
        String lowered = expr.toLowerCase(Locale.ROOT);
        int asIdx = lowered.lastIndexOf(" as ");
        if (asIdx > 0) {
            String alias = expr.substring(asIdx + 4).trim();
            return cleanIdentifier(alias);
        }
        List<String> parts = splitTopLevel(expr, ' ');
        if (parts.size() >= 2) {
            String alias = parts.get(parts.size() - 1).trim();
            if (!alias.isEmpty() && Character.isLetterOrDigit(alias.charAt(0))) {
                return cleanIdentifier(alias);
            }
        }
        return null;
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

    public record ProjectionItem(Type type, String tableAlias, String expression, String alias, String displayLabel) {
        public enum Type {STAR, EXPR}
    }

    public record Projection(List<ProjectionItem> items, boolean distinct, Map<String, String> tableAliases,
                             String tableForStar) {}
}
