package tools.sqlclient.importer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ClipboardRowSource implements RowSource {
    private final String raw;
    private List<String> header;
    private List<List<String>> rows;
    private Iterator<List<String>> iterator;

    public ClipboardRowSource(String raw) {
        this.raw = raw == null ? "" : raw;
        init();
    }

    private void init() {
        ClipboardTableParser.ParsedClipboard parsed = ClipboardTableParser.parse(raw);
        this.header = parsed.header();
        this.rows = parsed.rows();
        this.iterator = rows.iterator();
    }

    @Override
    public List<String> getHeader() {
        return header;
    }

    @Override
    public List<String> nextRow() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public RowSource reopen() {
        init();
        return this;
    }

    @Override
    public String getDescription() {
        int chars = raw.length();
        String preview = raw.length() > 200 ? raw.substring(0, 200) + "..." : raw;
        return "Clipboard(" + chars + " chars): " + preview.replaceAll("\n", " ");
    }

    @Override
    public void close() throws IOException {
        // nothing
    }
}
