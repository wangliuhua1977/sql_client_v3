package tools.sqlclient.util;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility to detect whether a selected SQL snippet should be treated as a single DDL block
 * (function/procedure/view definitions) instead of being split by semicolons.
 */
public class SqlBlockDetector {

    private static final Pattern DDL_START_PATTERN = Pattern.compile(
            "^(?i)(create|alter)\\s+(or\\s+replace\\s+)?(function|procedure|view|materialized\\s+view)"
    );

    private static final Pattern REFRESH_MATERIALIZED_VIEW = Pattern.compile(
            "^(?i)refresh\\s+materialized\\s+view"
    );

    private SqlBlockDetector() {
    }

    public static DdlBlockDetectionResult detectSingleDdlBlock(String selectionSql) {
        if (selectionSql == null) {
            return new DdlBlockDetectionResult(false, null);
        }
        String trimmed = selectionSql.trim();
        if (trimmed.isEmpty()) {
            return new DdlBlockDetectionResult(false, selectionSql);
        }

        if (!startsWithDdl(trimmed)) {
            return new DdlBlockDetectionResult(false, selectionSql);
        }

        StructureAnalysis analysis = analyzeStructure(selectionSql);
        if (!analysis.isBalanced()) {
            return new DdlBlockDetectionResult(false, selectionSql);
        }

        if (analysis.getTopLevelSemicolonCount() > 1) {
            return new DdlBlockDetectionResult(false, selectionSql);
        }

        if (analysis.getTopLevelSemicolonCount() == 1) {
            String remaining = selectionSql.substring(analysis.getLastTopLevelSemicolonIndex() + 1);
            remaining = stripComments(remaining).trim();
            if (!remaining.isEmpty()) {
                return new DdlBlockDetectionResult(false, selectionSql);
            }
        }

        return new DdlBlockDetectionResult(true, selectionSql);
    }

    private static boolean startsWithDdl(String trimmedSql) {
        Matcher ddlMatcher = DDL_START_PATTERN.matcher(trimmedSql);
        if (ddlMatcher.find()) {
            return true;
        }
        Matcher refreshMatcher = REFRESH_MATERIALIZED_VIEW.matcher(trimmedSql);
        return refreshMatcher.find();
    }

    private static StructureAnalysis analyzeStructure(String sql) {
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        String activeDollarQuote = null;
        int topLevelSemicolons = 0;
        int lastSemicolonIndex = -1;

        int length = sql.length();
        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);
            char next = i + 1 < length ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n' || c == '\r') {
                    inLineComment = false;
                }
                continue;
            }

            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }

            if (activeDollarQuote != null) {
                if (sql.startsWith(activeDollarQuote, i)) {
                    i += activeDollarQuote.length() - 1;
                    activeDollarQuote = null;
                }
                continue;
            }

            if (inSingleQuote) {
                if (c == '\'' && next == '\'') {
                    i++;
                } else if (c == '\'') {
                    inSingleQuote = false;
                }
                continue;
            }

            if (inDoubleQuote) {
                if (c == '"' && next == '"') {
                    i++;
                } else if (c == '"') {
                    inDoubleQuote = false;
                }
                continue;
            }

            if (c == '-' && next == '-') {
                inLineComment = true;
                i++;
                continue;
            }

            if (c == '/' && next == '*') {
                inBlockComment = true;
                i++;
                continue;
            }

            if (c == '$') {
                String tag = extractDollarTag(sql, i);
                if (tag != null) {
                    activeDollarQuote = tag;
                    i += tag.length() - 1;
                    continue;
                }
            }

            if (c == '\'') {
                inSingleQuote = true;
                continue;
            }

            if (c == '"') {
                inDoubleQuote = true;
                continue;
            }

            if (c == ';') {
                topLevelSemicolons++;
                lastSemicolonIndex = i;
            }
        }

        boolean balanced = !inSingleQuote && !inDoubleQuote && !inLineComment && !inBlockComment && activeDollarQuote == null;
        return new StructureAnalysis(balanced, topLevelSemicolons, lastSemicolonIndex);
    }

    private static String extractDollarTag(String sql, int index) {
        int length = sql.length();
        int end = index + 1;
        while (end < length) {
            char ch = sql.charAt(end);
            if (ch == '$') {
                return sql.substring(index, end + 1);
            }
            if (!Character.isLetterOrDigit(ch) && ch != '_') {
                return null;
            }
            end++;
        }
        return null;
    }

    private static String stripComments(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        String blockRemoved = input.replaceAll("(?s)/\\*.*?\\*/", " ");
        return blockRemoved.replaceAll("(?m)--.*$", " ");
    }

    public static class DdlBlockDetectionResult {
        private final boolean singleDdlBlock;
        private final String normalizedSql;

        public DdlBlockDetectionResult(boolean singleDdlBlock, String normalizedSql) {
            this.singleDdlBlock = singleDdlBlock;
            this.normalizedSql = normalizedSql;
        }

        public boolean isSingleDdlBlock() {
            return singleDdlBlock;
        }

        public String getNormalizedSql() {
            return normalizedSql;
        }

        @Override
        public String toString() {
            return "DdlBlockDetectionResult{" +
                    "singleDdlBlock=" + singleDdlBlock +
                    ", normalizedSql='" + Objects.toString(normalizedSql) + '\'' +
                    '}';
        }
    }

    private record StructureAnalysis(boolean balanced, int topLevelSemicolonCount, int lastTopLevelSemicolonIndex) {
        public boolean isBalanced() {
            return balanced;
        }

        public int getTopLevelSemicolonCount() {
            return topLevelSemicolonCount;
        }

        public int getLastTopLevelSemicolonIndex() {
            return lastTopLevelSemicolonIndex;
        }
    }
}
