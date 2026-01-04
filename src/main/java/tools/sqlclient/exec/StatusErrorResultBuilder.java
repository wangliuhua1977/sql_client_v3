package tools.sqlclient.exec;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从 /jobs/status 或 /jobs/result 的原始 JSON Body 构造用于表格展示的错误结果。
 */
public final class StatusErrorResultBuilder {
    private static final Pattern POSITION_PATTERN = Pattern.compile("(?i)\\bPosition:\\s*(\\d+)\\b");

    private StatusErrorResultBuilder() {
    }

    public static TableErrorFormatter.ErrorTable buildErrorResultTable(JsonObject body) {
        String errorMessage = extractString(body, "errorMessage");
        if (errorMessage == null || errorMessage.isBlank()) {
            errorMessage = "数据库未返回错误信息";
        }
        Integer position = extractInteger(body, "position");
        if (position == null) {
            position = extractInteger(body, "Position");
        }
        if (position == null) {
            position = extractPositionFromMessage(errorMessage);
        }

        List<ColumnDef> defs = new ArrayList<>();
        defs.add(new ColumnDef("ERROR_MESSAGE", "ERROR_MESSAGE", "ERROR_MESSAGE"));
        defs.add(new ColumnDef("POSITION", "POSITION", "POSITION"));

        Map<String, String> rowMap = new LinkedHashMap<>();
        rowMap.put("ERROR_MESSAGE", errorMessage);
        rowMap.put("POSITION", position != null ? String.valueOf(position) : "");

        List<String> row = new ArrayList<>();
        row.add(rowMap.get("ERROR_MESSAGE"));
        row.add(rowMap.get("POSITION"));

        String displayMessage = TableErrorFormatter.buildDisplayMessage(errorMessage, position);
        return new TableErrorFormatter.ErrorTable(defs, List.of(row), List.of(rowMap), displayMessage);
    }

    public static Integer extractPositionFromMessage(String errorMessage) {
        if (errorMessage == null) {
            return null;
        }
        Matcher matcher = POSITION_PATTERN.matcher(errorMessage);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static Integer extractInteger(JsonObject body, String field) {
        if (body == null || field == null) {
            return null;
        }
        JsonElement el = body.get(field);
        if (el == null || el.isJsonNull()) {
            return null;
        }
        try {
            return el.getAsInt();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String extractString(JsonObject body, String field) {
        if (body == null || field == null) {
            return null;
        }
        JsonElement el = body.get(field);
        if (el == null || el.isJsonNull()) {
            return null;
        }
        return el.getAsString();
    }
}

