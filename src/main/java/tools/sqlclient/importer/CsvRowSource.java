package tools.sqlclient.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 流式 CSV 读取，保留空列与尾随分隔符。
 */
public class CsvRowSource implements RowSource {
    private final Path csvFile;
    private final Charset charset;
    private final char delimiter;
    private final char quote;
    private BufferedReader reader;
    private List<String> headers;
    private long lineNumber;

    public CsvRowSource(Path csvFile, Charset charset, char delimiter, char quote) {
        this.csvFile = csvFile;
        this.charset = charset;
        this.delimiter = delimiter;
        this.quote = quote;
    }

    @Override
    public void open() throws IOException {
        this.reader = Files.newBufferedReader(csvFile, charset);
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
        List<String> cols = parseCsv(line);
        if (headers == null) {
            headers = cols;
            return nextRow();
        }
        ensureColumnCount(cols);
        return new RowData(lineNumber - 1, cols);
    }

    private List<String> parseCsv(String line) throws IOException {
        List<String> cols = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuote) {
                if (c == quote) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                        current.append(quote);
                        i++;
                    } else {
                        inQuote = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == quote) {
                    inQuote = true;
                } else if (c == delimiter) {
                    cols.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
        }
        cols.add(current.toString());
        if (inQuote) {
            throw new IOException("CSV 引号不匹配，第 " + lineNumber + " 行");
        }
        return cols;
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
