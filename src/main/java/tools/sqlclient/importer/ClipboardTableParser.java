package tools.sqlclient.importer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClipboardTableParser {
    public TableData parse(String text) throws ParseError {
        if (text == null || text.isBlank()) {
            throw new ParseError("内容为空，无法解析");
        }
        String normalized = text.replace("\r\n", "\n").replace('\r', '\n');
        List<String> lines = Arrays.stream(normalized.split("\n"))
                .collect(Collectors.toCollection(ArrayList::new));
        while (!lines.isEmpty() && lines.get(lines.size() - 1).trim().isEmpty()) {
            lines.remove(lines.size() - 1);
        }
        if (lines.isEmpty()) {
            throw new ParseError("内容为空，无法解析");
        }
        String delimiter = determineDelimiter(lines.get(0));
        List<String> header = splitLine(lines.get(0), delimiter);
        int colCount = header.size();
        if (colCount == 0) {
            throw new ParseError("标题列为空");
        }
        List<List<String>> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            List<String> cols = splitLine(lines.get(i), delimiter);
            if (cols.stream().allMatch(String::isBlank)) {
                continue;
            }
            if (cols.size() != colCount) {
                throw new ParseError(i + 1, colCount, cols.size());
            }
            rows.add(cols);
        }
        return new TableData(header, rows);
    }

    private String determineDelimiter(String headerLine) {
        if (headerLine.contains("\t")) {
            return "\t";
        }
        if (headerLine.contains(",")) {
            return ",";
        }
        return ";";
    }

    private List<String> splitLine(String line, String delimiter) {
        String[] arr = line.split(java.util.regex.Pattern.quote(delimiter), -1);
        List<String> result = new ArrayList<>(arr.length);
        for (String a : arr) {
            result.add(a);
        }
        return result;
    }
}
