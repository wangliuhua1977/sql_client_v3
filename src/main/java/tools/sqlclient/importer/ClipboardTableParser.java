package tools.sqlclient.importer;

import java.util.ArrayList;
import java.util.List;

/**
 * Strict parser for clipboard table text using tab-separated values.
 */
public final class ClipboardTableParser {
    private ClipboardTableParser() {
    }

    public static ParsedClipboard parse(String raw) {
        if (raw == null) {
            raw = "";
        }
        String[] lines = raw.replace("\r\n", "\n").replace('\r', '\n').split("\n");
        List<List<String>> rows = new ArrayList<>();
        int expectedCols = -1;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (line.isEmpty() && i == lines.length - 1) {
                continue;
            }
            List<String> cols = parseLine(line);
            if (expectedCols < 0) {
                expectedCols = cols.size();
            }
            if (cols.size() != expectedCols) {
                throw new IllegalArgumentException("行 " + (i + 1) + " 列数不一致：期望 " + expectedCols + " 实际 " + cols.size());
            }
            rows.add(cols);
        }
        if (rows.isEmpty()) {
            return new ParsedClipboard(List.of(), List.of());
        }
        List<String> header = rows.remove(0);
        return new ParsedClipboard(header, rows);
    }

    private static List<String> parseLine(String line) {
        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\t') {
                list.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        list.add(sb.toString());
        return list;
    }

    public record ParsedClipboard(List<String> header, List<List<String>> rows) {
    }
}
