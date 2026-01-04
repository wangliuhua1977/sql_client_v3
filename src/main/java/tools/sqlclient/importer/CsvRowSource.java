package tools.sqlclient.importer;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CsvRowSource implements RowSource {
    private final BufferedReader reader;
    private final char delimiter;
    private List<String> next;

    public CsvRowSource(String path, Charset charset, char delimiter) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), charset));
        this.delimiter = delimiter;
        this.next = readLine();
    }

    private List<String> readLine() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                quoted = !quoted;
                continue;
            }
            if (!quoted && c == delimiter) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        return result;
    }

    @Override
    public Iterator<List<String>> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public List<String> next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                List<String> current = next;
                try {
                    next = readLine();
                } catch (IOException e) {
                    next = null;
                }
                return current;
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
