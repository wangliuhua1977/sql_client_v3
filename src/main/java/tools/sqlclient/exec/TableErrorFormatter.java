package tools.sqlclient.exec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 构造表格错误展示文本的工具，优先使用后端返回的 errorMessage 与 position/Position 字段。
 */
public final class TableErrorFormatter {
    private TableErrorFormatter() {
    }

    public record ErrorTable(List<ColumnDef> columnDefs,
                             List<List<String>> rows,
                             List<Map<String, String>> rowMaps,
                             String displayMessage) {
    }

    /**
     * 根据结果响应生成表格中显示的错误文本。
     *
     * @param resp 后端返回的结果响应
     * @return 适用于结果表格的错误正文，多行使用 "\n" 分隔；若无可用信息则返回 null
     */
    public static String buildTableErrorText(ResultResponse resp) {
        ErrorTable table = buildErrorTable(resp);
        return table != null ? table.displayMessage : null;
    }

    public static ErrorTable buildErrorTable(ResultResponse resp) {
        if (resp == null) {
            return null;
        }

        boolean failed = isFailed(resp);
        if (!failed) {
            return null;
        }

        String errorMessage = resolveErrorMessage(resp);
        Integer position = resolvePosition(resp);
        String sqlState = normalize(resp.getError() != null ? resp.getError().getSqlState() : null);
        String detail = normalize(resp.getError() != null ? resp.getError().getDetail() : null);
        String hint = normalize(resp.getError() != null ? resp.getError().getHint() : null);

        List<ColumnDef> defs = new ArrayList<>();
        defs.add(new ColumnDef("ERROR_MESSAGE", "ERROR_MESSAGE", "ERROR_MESSAGE"));
        defs.add(new ColumnDef("POSITION", "POSITION", "POSITION"));
        List<String> columns = new ArrayList<>();
        columns.add("ERROR_MESSAGE");
        columns.add("POSITION");

        Map<String, String> rowMap = new LinkedHashMap<>();
        rowMap.put("ERROR_MESSAGE", errorMessage != null ? errorMessage : "数据库未返回错误信息");
        rowMap.put("POSITION", position != null ? String.valueOf(position) : "");

        if (sqlState != null) {
            defs.add(new ColumnDef("SQL_STATE", "SQL_STATE", "SQL_STATE"));
            columns.add("SQL_STATE");
            rowMap.put("SQL_STATE", sqlState);
        }
        if (detail != null) {
            defs.add(new ColumnDef("DETAIL", "DETAIL", "DETAIL"));
            columns.add("DETAIL");
            rowMap.put("DETAIL", detail);
        }
        if (hint != null) {
            defs.add(new ColumnDef("HINT", "HINT", "HINT"));
            columns.add("HINT");
            rowMap.put("HINT", hint);
        }

        String displayMessage = buildDisplayMessage(rowMap.get("ERROR_MESSAGE"), position);
        List<String> row = new ArrayList<>();
        for (String col : columns) {
            row.add(rowMap.getOrDefault(col, ""));
        }

        return new ErrorTable(defs, List.of(row), List.of(rowMap), displayMessage);
    }

    private static boolean isFailed(ResultResponse resp) {
        String status = normalize(resp.getStatus());
        return "FAILED".equalsIgnoreCase(status)
                || normalize(resp.getErrorMessage()) != null
                || normalize(extractErrorMessage(resp)) != null
                || resp.getPosition() != null
                || resp.getPositionUpper() != null
                || (resp.getError() != null && resp.getError().getPosition() != null);
    }

    private static String resolveErrorMessage(ResultResponse resp) {
        String errorMessage = normalize(resp.getErrorMessage());
        if (errorMessage != null) {
            return errorMessage;
        }
        String extracted = extractErrorMessage(resp);
        if (extracted != null) {
            return extracted;
        }
        String message = normalize(resp.getMessage());
        if (message != null) {
            return message;
        }
        return "数据库未返回错误信息";
    }

    private static String extractErrorMessage(ResultResponse resp) {
        if (resp == null || resp.getError() == null) {
            return null;
        }
        String raw = normalize(resp.getError().getRaw());
        if (raw != null) {
            return raw;
        }
        return normalize(resp.getError().getMessage());
    }

    private static Integer resolvePosition(ResultResponse resp) {
        if (resp.getPosition() != null && resp.getPosition() > 0) {
            return resp.getPosition();
        }
        if (resp.getPositionUpper() != null && resp.getPositionUpper() > 0) {
            return resp.getPositionUpper();
        }
        if (resp.getError() != null && resp.getError().getPosition() != null && resp.getError().getPosition() > 0) {
            return resp.getError().getPosition();
        }
        return null;
    }

    private static String buildDisplayMessage(String errorMessage, Integer position) {
        List<String> lines = new ArrayList<>();
        if (errorMessage != null && !errorMessage.isBlank()) {
            lines.add(errorMessage);
        }
        if (position != null && position > 0) {
            lines.add("Position: " + position);
        }
        return String.join("\n", lines);
    }

    private static String normalize(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}

