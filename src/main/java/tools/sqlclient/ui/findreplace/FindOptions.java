package tools.sqlclient.ui.findreplace;

import java.util.Objects;

public final class FindOptions {

    public enum Direction {
        FORWARD,
        BACKWARD
    }

    private final String query;
    private final boolean caseSensitive;
    private final boolean wholeWord;
    private final boolean regex;
    private final boolean wrap;
    private final Direction direction;

    public FindOptions(String query, boolean caseSensitive, boolean wholeWord, boolean regex, boolean wrap, Direction direction) {
        this.query = query == null ? "" : query;
        this.caseSensitive = caseSensitive;
        this.wholeWord = wholeWord;
        this.regex = regex;
        this.wrap = wrap;
        this.direction = Objects.requireNonNull(direction, "direction");
    }

    public String query() {
        return query;
    }

    public boolean caseSensitive() {
        return caseSensitive;
    }

    public boolean wholeWord() {
        return wholeWord;
    }

    public boolean regex() {
        return regex;
    }

    public boolean wrap() {
        return wrap;
    }

    public Direction direction() {
        return direction;
    }

    public FindOptions withDirection(Direction newDirection) {
        return new FindOptions(query, caseSensitive, wholeWord, regex, wrap, newDirection);
    }
}
