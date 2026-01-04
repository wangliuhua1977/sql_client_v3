package tools.sqlclient.importer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CsvRowSource implements RowSource {
    private final Path path;
    private final Charset charset;
    private final char delimiter;
    private final boolean hasHeader;
    private List<String> header;
    private CsvParser parser;
    private java.io.Reader reader;

    public CsvRowSource(Path path, Charset charset, char delimiter, boolean hasHeader) throws IOException {
        this.path = path;
        this.charset = charset;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
        open();
    }

    private void open() throws IOException {
        close();
        reader = new InputStreamReader(Files.newInputStream(path), charset);
        parser = new CsvParser(reader, delimiter);
        List<String> first = parser.next();
        if (first == null) {
            header = List.of();
        } else {
            if (hasHeader) {
                header = first;
            } else {
                List<String> generated = new java.util.ArrayList<>();
                for (int i = 0; i < first.size(); i++) {
                    generated.add("col_" + (i + 1));
                }
                header = generated;
                // restart parser so first row is not lost
                parser = new CsvParser(new InputStreamReader(Files.newInputStream(path), charset), delimiter);
            }
        }
    }

    @Override
    public List<String> getHeader() {
        return header;
    }

    @Override
    public List<String> nextRow() throws IOException {
        return parser.next();
    }

    @Override
    public RowSource reopen() throws IOException {
        open();
        return this;
    }

    @Override
    public String getDescription() {
        return "CSV:" + path.toAbsolutePath();
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
