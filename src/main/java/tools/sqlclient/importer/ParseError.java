package tools.sqlclient.importer;

public class ParseError extends Exception {
    private final int lineNumber;
    private final int expectedColumns;
    private final int actualColumns;

    public ParseError(String message) {
        super(message);
        this.lineNumber = -1;
        this.expectedColumns = -1;
        this.actualColumns = -1;
    }

    public ParseError(int lineNumber, int expectedColumns, int actualColumns) {
        super("第" + lineNumber + "行列数不一致：期望" + expectedColumns + "，实际" + actualColumns);
        this.lineNumber = lineNumber;
        this.expectedColumns = expectedColumns;
        this.actualColumns = actualColumns;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public int getExpectedColumns() {
        return expectedColumns;
    }

    public int getActualColumns() {
        return actualColumns;
    }
}
