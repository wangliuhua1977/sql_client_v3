package tools.sqlclient.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight CSV parser supporting quotes and embedded newlines.
 */
public class CsvParser {
    private final Reader reader;
    private final char delimiter;
    private final List<String> buffer = new ArrayList<>();
    private final StringBuilder cell = new StringBuilder();
    private boolean inQuote = false;

    public CsvParser(Reader reader, char delimiter) {
        this.reader = new BufferedReader(reader);
        this.delimiter = delimiter;
    }

    public List<String> next() throws IOException {
        buffer.clear();
        cell.setLength(0);
        int ch;
        boolean hasData = false;
        while ((ch = reader.read()) != -1) {
            hasData = true;
            char c = (char) ch;
            if (inQuote) {
                if (c == '"') {
                    reader.mark(1);
                    int next = reader.read();
                    if (next == '"') {
                        cell.append('"');
                    } else {
                        inQuote = false;
                        if (next != -1) {
                            reader.reset();
                        }
                    }
                } else {
                    cell.append(c);
                }
            } else {
                if (c == '"') {
                    inQuote = true;
                } else if (c == delimiter) {
                    buffer.add(cell.toString());
                    cell.setLength(0);
                } else if (c == '\n') {
                    buffer.add(cell.toString());
                    return new ArrayList<>(buffer);
                } else if (c == '\r') {
                    // skip
                } else {
                    cell.append(c);
                }
            }
        }
        if (!hasData && cell.isEmpty()) {
            return null;
        }
        buffer.add(cell.toString());
        return new ArrayList<>(buffer);
    }
}
