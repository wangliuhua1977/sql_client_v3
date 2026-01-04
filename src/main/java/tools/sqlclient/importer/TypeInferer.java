package tools.sqlclient.importer;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class TypeInferer {
    public List<String> infer(TableData data, boolean emptyAsNull) {
        int cols = data.getColumnCount();
        List<String> types = new ArrayList<>(cols);
        for (int c = 0; c < cols; c++) {
            types.add("text");
        }
        for (int c = 0; c < cols; c++) {
            String decided = decideTypeForColumn(data, c, emptyAsNull);
            types.set(c, decided);
        }
        return types;
    }

    private String decideTypeForColumn(TableData data, int colIndex, boolean emptyAsNull) {
        String current = "boolean";
        for (List<String> row : data.getRows()) {
            String value = colIndex < row.size() ? row.get(colIndex) : "";
            if (value == null) {
                continue;
            }
            String trimmed = value.trim();
            if (trimmed.isEmpty() && emptyAsNull) {
                continue;
            }
            String next = upgradeType(current, trimmed);
            current = next;
            if ("text".equals(current)) {
                break;
            }
        }
        return current;
    }

    private String upgradeType(String current, String value) {
        String lower = value.toLowerCase(Locale.ROOT);
        switch (current) {
            case "boolean":
                if (isBoolean(lower)) {
                    return "boolean";
                }
            case "integer":
                if (isInteger(value)) {
                    return "integer";
                }
            case "bigint":
                if (isBigint(value)) {
                    return "bigint";
                }
            case "numeric":
                if (isNumeric(value)) {
                    return "numeric";
                }
            case "date":
                if (isDate(value)) {
                    return "date";
                }
            case "timestamp":
                if (isTimestamp(value)) {
                    return "timestamp";
                }
            default:
                return "text";
        }
    }

    private boolean isBoolean(String lower) {
        return lower.equals("true") || lower.equals("false") || lower.equals("t") || lower.equals("f")
                || lower.equals("yes") || lower.equals("no") || lower.equals("y") || lower.equals("n")
                || lower.equals("1") || lower.equals("0");
    }

    private boolean isInteger(String v) {
        try {
            long parsed = Long.parseLong(v);
            return parsed >= Integer.MIN_VALUE && parsed <= Integer.MAX_VALUE;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private boolean isBigint(String v) {
        try {
            Long.parseLong(v);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private boolean isNumeric(String v) {
        try {
            new BigDecimal(v);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private boolean isDate(String v) {
        return parseWithPatterns(v, new String[]{"yyyy-MM-dd", "yyyy/MM/dd", "yyyy.MM.dd"}, LocalDate::parse);
    }

    private boolean isTimestamp(String v) {
        return parseWithPatterns(v, new String[]{
                "yyyy-MM-dd HH:mm:ss",
                "yyyy/MM/dd HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss"
        }, LocalDateTime::parse);
    }

    private boolean parseWithPatterns(String value, String[] patterns, java.util.function.BiFunction<CharSequence, DateTimeFormatter, ?> parser) {
        for (String p : patterns) {
            try {
                parser.apply(value, DateTimeFormatter.ofPattern(p));
                return true;
            } catch (DateTimeParseException ignored) {
            }
        }
        return false;
    }
}
