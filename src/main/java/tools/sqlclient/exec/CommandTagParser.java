package tools.sqlclient.exec;

import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析 PostgreSQL commandTag，提取受影响行数。
 */
public class CommandTagParser {
    private static final Pattern AFFECTED_PATTERN = Pattern.compile("^(INSERT|UPDATE|DELETE|SELECT)\\s+(\\d+)$", Pattern.CASE_INSENSITIVE);

    public static OptionalInt parseAffectedRows(String commandTag) {
        if (commandTag == null) {
            return OptionalInt.empty();
        }
        Matcher matcher = AFFECTED_PATTERN.matcher(commandTag.trim());
        if (matcher.matches()) {
            try {
                return OptionalInt.of(Integer.parseInt(matcher.group(2)));
            } catch (NumberFormatException ignored) {
            }
        }
        return OptionalInt.empty();
    }
}
