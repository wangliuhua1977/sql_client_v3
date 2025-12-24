package tools.sqlclient.exec;

import java.util.Set;

/**
 * 轻量级 SQL 顶层语句分类器，基于首关键字判断是否返回结果集。
 */
public class SqlTopLevelClassifier {
    public enum TopLevelType {
        RESULT_SET,
        NON_QUERY,
        UNKNOWN
    }

    private static final Set<String> RESULT_SET_KEYWORDS = Set.of("SELECT", "VALUES", "SHOW", "EXPLAIN");
    private static final Set<String> NON_QUERY_KEYWORDS = Set.of(
            "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE",
            "GRANT", "REVOKE", "MERGE", "CALL", "SET", "DO", "VACUUM", "ANALYZE",
            "REFRESH", "COPY"
    );

    public static TopLevelType classify(String sql) {
        if (sql == null || sql.isBlank()) {
            return TopLevelType.UNKNOWN;
        }
        int idx = skipIgnorable(sql, 0);
        if (idx >= sql.length()) {
            return TopLevelType.UNKNOWN;
        }
        Token token = readNextToken(sql, idx);
        if (token == null) {
            return TopLevelType.UNKNOWN;
        }
        String upper = token.text.toUpperCase();
        if ("WITH".equals(upper)) {
            return classifyAfterWith(sql, token.nextIndex);
        }
        return classifyKeyword(upper);
    }

    private static TopLevelType classifyAfterWith(String sql, int startIdx) {
        int idx = startIdx;
        int parenDepth = 0;
        while (idx < sql.length()) {
            idx = skipIgnorable(sql, idx);
            if (idx >= sql.length()) {
                break;
            }
            char ch = sql.charAt(idx);
            if (ch == '(') {
                parenDepth++;
                idx++;
                continue;
            }
            if (ch == ')') {
                parenDepth = Math.max(0, parenDepth - 1);
                idx++;
                continue;
            }
            if (ch == '\'' || ch == '"') {
                idx = skipQuoted(sql, idx);
                continue;
            }
            if (Character.isLetter(ch)) {
                Token token = readNextToken(sql, idx);
                if (token == null) {
                    break;
                }
                String upper = token.text.toUpperCase();
                if ("RECURSIVE".equals(upper) && parenDepth == 0) {
                    idx = token.nextIndex;
                    continue;
                }
                if (parenDepth == 0) {
                    TopLevelType type = classifyKeyword(upper);
                    if (type != TopLevelType.UNKNOWN) {
                        return type;
                    }
                }
                idx = token.nextIndex;
                continue;
            }
            idx++;
        }
        return TopLevelType.UNKNOWN;
    }

    private static TopLevelType classifyKeyword(String upper) {
        if (RESULT_SET_KEYWORDS.contains(upper)) {
            return TopLevelType.RESULT_SET;
        }
        if (NON_QUERY_KEYWORDS.contains(upper)) {
            return TopLevelType.NON_QUERY;
        }
        return TopLevelType.UNKNOWN;
    }

    private static int skipIgnorable(String sql, int from) {
        int idx = from;
        while (idx < sql.length()) {
            char ch = sql.charAt(idx);
            if (Character.isWhitespace(ch)) {
                idx++;
                continue;
            }
            if (ch == '-' && idx + 1 < sql.length() && sql.charAt(idx + 1) == '-') {
                idx = skipLineComment(sql, idx + 2);
                continue;
            }
            if (ch == '/' && idx + 1 < sql.length() && sql.charAt(idx + 1) == '*') {
                idx = skipBlockComment(sql, idx + 2);
                continue;
            }
            break;
        }
        return idx;
    }

    private static int skipLineComment(String sql, int from) {
        int idx = from;
        while (idx < sql.length() && sql.charAt(idx) != '\n') {
            idx++;
        }
        return idx;
    }

    private static int skipBlockComment(String sql, int from) {
        int idx = from;
        while (idx + 1 < sql.length()) {
            if (sql.charAt(idx) == '*' && sql.charAt(idx + 1) == '/') {
                return idx + 2;
            }
            idx++;
        }
        return sql.length();
    }

    private static int skipQuoted(String sql, int from) {
        char quote = sql.charAt(from);
        int idx = from + 1;
        while (idx < sql.length()) {
            char ch = sql.charAt(idx);
            if (ch == quote) {
                if (idx + 1 < sql.length() && sql.charAt(idx + 1) == quote) {
                    idx += 2;
                    continue;
                }
                return idx + 1;
            }
            if (ch == '\\' && quote == '\'' && idx + 1 < sql.length()) {
                idx += 2;
                continue;
            }
            idx++;
        }
        return sql.length();
    }

    private static Token readNextToken(String sql, int from) {
        int idx = skipIgnorable(sql, from);
        if (idx >= sql.length()) {
            return null;
        }
        char ch = sql.charAt(idx);
        if (!Character.isLetter(ch)) {
            return null;
        }
        int end = idx + 1;
        while (end < sql.length()) {
            char c = sql.charAt(end);
            if (Character.isLetterOrDigit(c) || c == '_') {
                end++;
            } else {
                break;
            }
        }
        return new Token(sql.substring(idx, end), end);
    }

    private record Token(String text, int nextIndex) {
    }
}
