package tools.sqlclient.importer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于样本的轻量类型推断。
 */
public class TypeInferer {
    private static final DateTimeFormatter[] DATE_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    };

    public List<ColumnSpec> infer(List<String> headers, List<List<String>> sampleRows) {
        List<String> normalized = ColumnNameNormalizer.normalize(headers);
        List<ColumnSpec> specs = new ArrayList<>();
        for (int col = 0; col < headers.size(); col++) {
            String type = inferTypeForColumn(sampleRows, col);
            specs.add(new ColumnSpec(headers.get(col), normalized.get(col), type));
        }
        return specs;
    }

    private String inferTypeForColumn(List<List<String>> sampleRows, int columnIndex) {
        boolean allInt = true;
        boolean allLong = true;
        boolean allBool = true;
        boolean allDate = true;
        boolean allTimestamp = true;
        for (List<String> row : sampleRows) {
            if (columnIndex >= row.size()) {
                continue;
            }
            String v = row.get(columnIndex);
            if (v == null || v.isEmpty()) {
                continue;
            }
            if (!looksLikeBoolean(v)) {
                allBool = false;
            }
            if (!looksLikeInteger(v)) {
                allInt = false;
            }
            if (!looksLikeLong(v)) {
                allLong = false;
            }
            if (!looksLikeDate(v)) {
                allDate = false;
            }
            if (!looksLikeTimestamp(v)) {
                allTimestamp = false;
            }
        }
        if (allBool) {
            return "boolean";
        }
        if (allInt) {
            return "integer";
        }
        if (allLong) {
            return "bigint";
        }
        if (allDate) {
            return "date";
        }
        if (allTimestamp) {
            return "timestamp";
        }
        return "text";
    }

    private boolean looksLikeBoolean(String v) {
        String lower = v.toLowerCase();
        return lower.equals("true") || lower.equals("false") || lower.equals("t") || lower.equals("f") || lower.equals("1") || lower.equals("0");
    }

    private boolean looksLikeInteger(String v) {
        try {
            Integer.parseInt(v);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean looksLikeLong(String v) {
        try {
            Long.parseLong(v);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean looksLikeDate(String v) {
        for (DateTimeFormatter fmt : DATE_FORMATS) {
            try {
                LocalDate.parse(v, fmt);
                return true;
            } catch (DateTimeParseException ignored) {
            }
        }
        return false;
    }

    private boolean looksLikeTimestamp(String v) {
        for (DateTimeFormatter fmt : DATE_FORMATS) {
            try {
                LocalDateTime.parse(v, fmt);
                return true;
            } catch (DateTimeParseException ignored) {
            }
        }
        return false;
    }
}
