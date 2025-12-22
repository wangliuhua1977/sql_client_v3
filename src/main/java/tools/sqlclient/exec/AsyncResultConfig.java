package tools.sqlclient.exec;

import tools.sqlclient.util.Config;

/**
 * 读取异步结果获取相关的可调参数。
 */
public class AsyncResultConfig {
    private static final int DEFAULT_RETRY_MAX = 8;
    private static final int DEFAULT_RETRY_BASE_DELAY_MS = 150;
    private static final int DEFAULT_RETRY_MAX_DELAY_MS = 1000;
    private static final String DEFAULT_PAGE_BASE = "AUTO";
    private static final String DEFAULT_REMOVE_AFTER = "AUTO";

    private AsyncResultConfig() {
    }

    public static int getRetryMaxAttempts() {
        return readInt("ASYNC_SQL_RESULT_RETRY_MAX", DEFAULT_RETRY_MAX);
    }

    public static int getRetryBaseDelayMs() {
        return readInt("ASYNC_SQL_RESULT_RETRY_BASE_DELAY_MS", DEFAULT_RETRY_BASE_DELAY_MS);
    }

    public static int getRetryMaxDelayMs() {
        return readInt("ASYNC_SQL_RESULT_RETRY_MAX_DELAY_MS", DEFAULT_RETRY_MAX_DELAY_MS);
    }

    public static PageBase getPageBase() {
        String raw = readString("ASYNC_SQL_RESULT_PAGE_BASE", DEFAULT_PAGE_BASE);
        if (raw == null) {
            return PageBase.AUTO;
        }
        return switch (raw.trim().toUpperCase()) {
            case "0", "ZERO" -> PageBase.ZERO_BASED;
            case "1", "ONE" -> PageBase.ONE_BASED;
            default -> PageBase.AUTO;
        };
    }

    public static RemoveAfterFetchStrategy getRemoveAfterFetchStrategy() {
        String raw = readString("ASYNC_SQL_RESULT_FETCH_REMOVE_AFTER", DEFAULT_REMOVE_AFTER);
        if (raw == null) {
            return RemoveAfterFetchStrategy.AUTO;
        }
        return switch (raw.trim().toUpperCase()) {
            case "TRUE", "YES", "ON" -> RemoveAfterFetchStrategy.TRUE;
            case "FALSE", "NO", "OFF" -> RemoveAfterFetchStrategy.FALSE;
            default -> RemoveAfterFetchStrategy.AUTO;
        };
    }

    private static int readInt(String key, int defaultValue) {
        String sys = System.getProperty(key);
        if (sys != null && !sys.isBlank()) {
            try {
                return Integer.parseInt(sys.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        String raw = Config.getRawProperty(key);
        if (raw != null && !raw.isBlank()) {
            try {
                return Integer.parseInt(raw.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private static String readString(String key, String defaultValue) {
        String sys = System.getProperty(key);
        if (sys != null && !sys.isBlank()) {
            return sys.trim();
        }
        String raw = Config.getRawProperty(key);
        if (raw != null && !raw.isBlank()) {
            return raw.trim();
        }
        return defaultValue;
    }

    public enum PageBase {
        ZERO_BASED,
        ONE_BASED,
        AUTO
    }

    public enum RemoveAfterFetchStrategy {
        AUTO,
        TRUE,
        FALSE
    }
}
