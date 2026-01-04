package tools.sqlclient.exec;

import java.util.ArrayList;
import java.util.List;

/**
 * 构造表格错误展示文本的工具，优先使用后端返回的 errorMessage 与 position/Position 字段。
 */
public final class TableErrorFormatter {
    private TableErrorFormatter() {
    }

    /**
     * 根据结果响应生成表格中显示的错误文本。
     *
     * @param resp 后端返回的结果响应
     * @return 适用于结果表格的错误正文，多行使用 "\n" 分隔；若无可用信息则返回 null
     */
    public static String buildTableErrorText(ResultResponse resp) {
        if (resp == null) {
            return null;
        }

        String status = normalize(resp.getStatus());
        boolean failed = "FAILED".equalsIgnoreCase(status)
                || normalize(resp.getErrorMessage()) != null
                || normalize(extractErrorMessage(resp)) != null
                || resp.getPosition() != null
                || (resp.getError() != null && resp.getError().getPosition() != null);
        if (!failed) {
            return null;
        }

        String errorMessage = normalize(resp.getErrorMessage());
        if (errorMessage == null) {
            errorMessage = extractErrorMessage(resp);
        }

        Integer position = resolvePosition(resp);

        List<String> lines = new ArrayList<>();
        if (errorMessage != null) {
            lines.add(errorMessage);
        }
        if (position != null && position > 0) {
            lines.add("Position: " + position);
        }

        if (!lines.isEmpty()) {
            return String.join("\n", lines);
        }

        if (resp.getJobId() != null && status != null) {
            return "任务" + resp.getJobId() + " 状态 " + status;
        }
        if (status != null) {
            return status;
        }
        return null;
    }

    private static String extractErrorMessage(ResultResponse resp) {
        if (resp == null || resp.getError() == null) {
            return null;
        }
        String message = normalize(resp.getError().getMessage());
        if (message != null) {
            return message;
        }
        return normalize(resp.getError().getRaw());
    }

    private static Integer resolvePosition(ResultResponse resp) {
        if (resp.getPosition() != null && resp.getPosition() > 0) {
            return resp.getPosition();
        }
        if (resp.getError() != null && resp.getError().getPosition() != null && resp.getError().getPosition() > 0) {
            return resp.getError().getPosition();
        }
        return null;
    }

    private static String normalize(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}

