package tools.sqlclient.util;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL 语句与代码块解析工具，忽略字符串、注释与 dollar-quote 内部的分号。
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

    public static List<SqlBlock> splitToBlocks(String fullText) {
        if (fullText == null || fullText.isBlank()) {
            return List.of();
        }
        List<SqlBlock> blocks = new ArrayList<>();
        int length = fullText.length();
        int cursor = 0;
        int blockStart = -1;
        while (cursor <= length) {
            int lineStart = cursor;
            int lineBreak = fullText.indexOf('\n', cursor);
            if (lineBreak < 0) {
                lineBreak = length;
            }
            int lineEnd = lineBreak;
            if (lineEnd > lineStart && fullText.charAt(lineEnd - 1) == '\r') {
                lineEnd -= 1;
            }
            String lineText = fullText.substring(lineStart, Math.max(lineStart, lineEnd));
            boolean isBlank = lineText.trim().isEmpty();
            if (isBlank) {
                if (blockStart >= 0) {
                    blocks.add(new SqlBlock(blockStart, lineStart, fullText.substring(blockStart, lineStart)));
                    blockStart = -1;
                }
            } else {
                if (blockStart < 0) {
                    blockStart = lineStart;
                }
            }
            if (lineBreak == length) {
                break;
            }
            cursor = lineBreak + 1;
        }
        if (blockStart >= 0) {
            blocks.add(new SqlBlock(blockStart, length, fullText.substring(blockStart)));
        }
        return blocks;
    }

    public static SqlBlock findBlockAtCaret(String fullText, int caretOffset) {
        List<SqlBlock> blocks = splitToBlocks(fullText);
        if (blocks.isEmpty()) {
            return null;
        }
        int caret = Math.max(0, Math.min(caretOffset, fullText.length()));
        for (SqlBlock block : blocks) {
            if (caret >= block.startOffset() && caret < block.endOffset()) {
                return block;
            }
            if (caret == fullText.length() && caret == block.endOffset()) {
                return block;
            }
        }
        int lineStart = lineStart(fullText, caret);
        int lineEnd = lineEnd(fullText, caret);
        String lineText = fullText.substring(lineStart, lineEnd);
        if (lineText.trim().isEmpty()) {
            for (SqlBlock block : blocks) {
                if (block.startOffset() > caret) {
                    return block;
                }
            }
            for (int i = blocks.size() - 1; i >= 0; i--) {
                SqlBlock block = blocks.get(i);
                if (block.endOffset() < caret) {
                    return block;
                }
            }
            return null;
        }
        return null;
    }

    public static List<String> splitBlockToStatements(String blockText) {
        if (blockText == null || blockText.isBlank()) {
            return List.of();
        }
        return split(blockText).stream()
                .map(SqlStatement::text)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .toList();
    }

    private static int lineStart(String text, int offset) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        int safe = Math.max(0, Math.min(offset, text.length()));
        int idx = text.lastIndexOf('\n', Math.max(0, safe - 1));
        if (idx < 0) {
            return 0;
        }
        return idx + 1;
    }

    private static int lineEnd(String text, int offset) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        int safe = Math.max(0, Math.min(offset, text.length()));
        int idx = text.indexOf('\n', safe);
        if (idx < 0) {
            idx = text.length();
        }
        if (idx > 0 && text.charAt(idx - 1) == '\r') {
            return idx - 1;
        }
        return idx;
    }

    public record SqlStatement(String text, int startOffset, int endOffset, boolean terminatedBySemicolon) {
    }

    public record SqlBlock(int startOffset, int endOffset, String text) {
    }
}
