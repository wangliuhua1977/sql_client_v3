package tools.sqlclient.exec;

/**
 * 错误展示文案选择器，集中处理新旧字段的兼容逻辑，便于测试。
 */
public final class ErrorDisplayFormatter {
    private ErrorDisplayFormatter() {
    }

    public static String chooseDisplayMessage(DatabaseErrorInfo errorInfo, String legacyErrorMessage, String legacyMessage, String statusText) {
        String candidate = normalize(errorInfo != null ? errorInfo.getRaw() : null);
        if (candidate != null) {
            return candidate;
        }
        candidate = normalize(legacyErrorMessage);
        if (candidate != null) {
            return candidate;
        }
        candidate = normalize(legacyMessage);
        if (candidate != null) {
            return candidate;
        }
        return normalize(statusText);
    }

    private static String normalize(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
