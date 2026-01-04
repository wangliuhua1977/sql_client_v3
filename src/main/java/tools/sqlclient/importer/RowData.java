package tools.sqlclient.importer;

import java.util.List;

/**
 * 表示一行数据。
 */
public class RowData {
    private final long lineNumber;
    private final List<String> values;

    public RowData(long lineNumber, List<String> values) {
        this.lineNumber = lineNumber;
        this.values = values;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public List<String> getValues() {
        return values;
    }
}
