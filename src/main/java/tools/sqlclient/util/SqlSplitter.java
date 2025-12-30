package tools.sqlclient.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 基于分号的 SQL 语句切分与 block 分组工具，忽略字符串与注释内部的分号。
 */
public final class SqlSplitter {

    private SqlSplitter() {
    }

    public static List<SqlStatement> split(String sqlText) {
        if (sqlText == null || sqlText.isBlank()) {
            return List.of();
        }
        String text = sqlText;
        List<SqlStatement> statements = new ArrayList<>();
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        String activeDollarQuote = null;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            char next = i + 1 < text.length() ? text.charAt(i + 1) : '\0';

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
                if (text.startsWith(activeDollarQuote, i)) {
                    i += activeDollarQuote.length() - 1;
                    activeDollarQuote = null;
                }
                continue;
            }
            if (inSingle) {
                if (c == '\'' && next == '\'') {
                    i++;
                } else if (c == '\'') {
                    inSingle = false;
                }
                continue;
            }
            if (inDouble) {
                if (c == '"' && next == '"') {
                    i++;
                } else if (c == '"') {
                    inDouble = false;
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
                String tag = extractDollarTag(text, i);
                if (tag != null) {
                    activeDollarQuote = tag;
                    i += tag.length() - 1;
                    continue;
                }
            }
            if (c == '\'') {
                inSingle = true;
                continue;
            }
            if (c == '"') {
                inDouble = true;
                continue;
            }
            if (c == ';') {
                addStatement(text, statements, start, i, true);
                start = i + 1;
            }
        }
        addStatement(text, statements, start, text.length(), false);
        return statements;
    }

    private static void addStatement(String text, List<SqlStatement> statements, int rawStart, int rawEnd, boolean terminated) {
        if (rawStart >= rawEnd) {
            return;
        }
        int start = rawStart;
        while (start < rawEnd && Character.isWhitespace(text.charAt(start))) {
            start++;
        }
        int end = rawEnd;
        while (end > start && Character.isWhitespace(text.charAt(end - 1))) {
            end--;
        }
        if (start >= end) {
            return;
        }
        String segment = text.substring(start, end);
        statements.add(new SqlStatement(segment, start, end, terminated));
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

    public static List<SqlBlock> groupIntoBlocks(String fullText, List<SqlStatement> statements) {
        if (statements == null || statements.isEmpty()) {
            return List.of();
        }
        List<LineRange> isolationLines = findIsolationLines(fullText);
        List<SqlBlock> blocks = new ArrayList<>();
        List<SqlStatement> current = new ArrayList<>();
        int blockStart = lineStart(fullText, statements.get(0).startOffset());
        for (int i = 0; i < statements.size(); i++) {
            SqlStatement stmt = statements.get(i);
            if (!current.isEmpty() && hasIsolationBetween(current.get(current.size() - 1), stmt, isolationLines)) {
                OperationLog.log("SQL_SPLIT_WARNING: blank line between statements ignored by rule");
                int blockEnd = current.get(current.size() - 1).endOffset();
                blocks.add(new SqlBlock(List.copyOf(current), blockStart, blockEnd));
                current.clear();
                blockStart = lineStart(fullText, stmt.startOffset());
            }
            current.add(stmt);
        }
        if (!current.isEmpty()) {
            int blockEnd = current.get(current.size() - 1).endOffset();
            blocks.add(new SqlBlock(List.copyOf(current), blockStart, blockEnd));
        }
        return blocks;
    }

    private static boolean hasIsolationBetween(SqlStatement left, SqlStatement right, List<LineRange> isolationLines) {
        int gapStart = left.endOffset();
        int gapEnd = right.startOffset();
        for (LineRange lr : isolationLines) {
            if (lr.intersects(gapStart, gapEnd)) {
                return true;
            }
        }
        return false;
    }

    private static List<LineRange> findIsolationLines(String fullText) {
        if (fullText == null || fullText.isEmpty()) {
            return List.of();
        }
        List<LineRange> ranges = new ArrayList<>();
        int offset = 0;
        String[] lines = fullText.split("\\r?\\n", -1);
        for (String line : lines) {
            int lengthWithBreak = line.length() + 1;
            int start = offset;
            int end = offset + line.length();
            if (isIsolationLine(line)) {
                ranges.add(new LineRange(start, offset + line.length()));
            }
            offset += lengthWithBreak;
        }
        return ranges;
    }

    public static SqlBlock findBlockAtCaret(int caretOffset, String fullText, List<SqlBlock> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return null;
        }
        int caret = Math.max(0, caretOffset);
        for (SqlBlock block : blocks) {
            if (caret >= block.blockStartOffset() && caret <= block.blockEndOffset()) {
                return block;
            }
        }
        for (int i = 0; i < blocks.size() - 1; i++) {
            SqlBlock first = blocks.get(i);
            SqlBlock second = blocks.get(i + 1);
            if (caret > first.blockEndOffset() && caret < second.blockStartOffset()) {
                return !first.statements().isEmpty() ? first : second;
            }
        }
        if (caret < blocks.get(0).blockStartOffset()) {
            return blocks.get(0);
        }
        return blocks.get(blocks.size() - 1);
    }

    public static SqlStatement findStatementAtCaret(int caretOffset, List<SqlStatement> statements) {
        if (statements == null || statements.isEmpty()) {
            return null;
        }
        int caret = Math.max(0, caretOffset);
        SqlStatement candidate = null;
        for (SqlStatement stmt : statements) {
            if (caret >= stmt.startOffset() && caret <= stmt.endOffset()) {
                return stmt;
            }
            if (stmt.startOffset() < caret) {
                candidate = stmt;
            }
        }
        return candidate;
    }

    public static boolean isIsolationLine(String lineText) {
        if (lineText == null) {
            return false;
        }
        return lineText.trim().isEmpty();
    }

    private static int lineStart(String text, int offset) {
        int safe = Math.max(0, Math.min(offset, text.length()));
        int idx = text.lastIndexOf('\n', safe);
        if (idx < 0) {
            return 0;
        }
        return idx + 1;
    }

    public record SqlStatement(String text, int startOffset, int endOffset, boolean terminatedBySemicolon) {
    }

    public record SqlBlock(List<SqlStatement> statements, int blockStartOffset, int blockEndOffset) {
        public List<SqlStatement> statements() {
            return statements == null ? Collections.emptyList() : statements;
        }
    }

    private record LineRange(int start, int end) {
        boolean intersects(int from, int to) {
            return Math.max(start, from) <= Math.min(end, to);
        }
    }
}
