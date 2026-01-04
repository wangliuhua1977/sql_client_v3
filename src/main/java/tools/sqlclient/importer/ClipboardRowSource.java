package tools.sqlclient.importer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ClipboardRowSource implements RowSource {
    private final List<List<String>> rows = new ArrayList<>();

    public ClipboardRowSource(String text) {
        if (text != null && !text.isEmpty()) {
            String[] lineArr = text.split("\r?\n");
            for (String line : lineArr) {
                String[] cols = line.split("\t", -1);
                rows.add(Arrays.asList(cols));
            }
        }
    }

    @Override
    public Iterator<List<String>> iterator() {
        return rows.iterator();
    }

    @Override
    public void close() throws IOException {
        rows.clear();
    }
}
