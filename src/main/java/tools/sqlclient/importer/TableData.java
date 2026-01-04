package tools.sqlclient.importer;

import java.util.ArrayList;
import java.util.List;

public class TableData {
    private final List<String> rawHeaders;
    private final List<List<String>> rows;

    public TableData(List<String> rawHeaders, List<List<String>> rows) {
        this.rawHeaders = new ArrayList<>(rawHeaders);
        this.rows = new ArrayList<>(rows);
    }

    public List<String> getRawHeaders() {
        return rawHeaders;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public int getColumnCount() {
        return rawHeaders.size();
    }

    public int getRowCount() {
        return rows.size();
    }
}
