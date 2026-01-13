package tools.sqlclient.ui.findreplace;

public final class FindResult {
    private final boolean found;
    private final int start;
    private final int end;
    private final int index;
    private final int total;
    private final boolean wrapped;
    private final String errorMessage;

    private FindResult(boolean found, int start, int end, int index, int total, boolean wrapped, String errorMessage) {
        this.found = found;
        this.start = start;
        this.end = end;
        this.index = index;
        this.total = total;
        this.wrapped = wrapped;
        this.errorMessage = errorMessage;
    }

    public static FindResult found(int start, int end, int index, int total, boolean wrapped) {
        return new FindResult(true, start, end, index, total, wrapped, null);
    }

    public static FindResult notFound(boolean wrapped) {
        return new FindResult(false, -1, -1, -1, -1, wrapped, null);
    }

    public static FindResult error(String message) {
        return new FindResult(false, -1, -1, -1, -1, false, message);
    }

    public boolean found() {
        return found;
    }

    public int start() {
        return start;
    }

    public int end() {
        return end;
    }

    public int index() {
        return index;
    }

    public int total() {
        return total;
    }

    public boolean wrapped() {
        return wrapped;
    }

    public String errorMessage() {
        return errorMessage;
    }
}
