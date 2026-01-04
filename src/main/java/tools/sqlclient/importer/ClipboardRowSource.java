package tools.sqlclient.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基于粘贴文本的行源，按 TAB 分隔，保持列对齐。
 */
public class ClipboardRowSource implements RowSource {
    private final String rawText;
    private BufferedReader reader;
    private List<String> headers;
    private long lineNumber = 0;

    public ClipboardRowSource(String rawText) {
        this.rawText = rawText == null ? "" : rawText;
    }

    @Override
    public void open() {
        this.reader = new BufferedReader(new StringReader(rawText));
        this.headers = null;
        this.lineNumber = 0;
    }

    @Override
    public List<String> getHeaders() {
        return headers == null ? List.of() : headers;
    }

    @Override
    public RowData nextRow() throws IOException {
        if (reader == null) {
            return null;
        }
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        lineNumber++;
        List<String> cols = parseLine(line);
        if (headers == null) {
            headers = cols;
            return nextRow();
        }
        ensureColumnCount(cols);
        return new RowData(lineNumber - 1, cols);
    }

    private List<String> parseLine(String line) {
        String[] parts = line.split("\t", -1);
        return new ArrayList<>(Arrays.asList(parts));
    }

    private void ensureColumnCount(List<String> cols) {
        if (headers == null) {
            return;
        }
        if (cols.size() != headers.size()) {
            throw new IllegalStateException("列数不一致: 期望 " + headers.size() + " 实际 " + cols.size());
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
