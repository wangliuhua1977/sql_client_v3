package tools.sqlclient.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.sqlclient.util.OperationLog;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 读取通用配置（config.properties）。
 */
public class Config {
    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private static final int DEFAULT_MAX_PAGE_SIZE = 1000;
    private static final int DEFAULT_PAGE_SIZE = 200;
    private static final Properties PROPS = new Properties();
    private static volatile Integer runtimePageSizeOverride;

    static {
        loadFromClasspath();
        loadFromWorkingDir();
    }

    private Config() {
    }

    private static void loadFromClasspath() {
        try (InputStream in = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in != null) {
                PROPS.load(in);
                log.info("已从 classpath 读取 config.properties");
            }
        } catch (Exception e) {
            log.warn("读取 classpath 配置失败: {}", e.getMessage());
        }
    }

    private static void loadFromWorkingDir() {
        Path path = Path.of("config.properties");
        if (!Files.exists(path)) {
            return;
        }
        try (InputStream in = Files.newInputStream(path)) {
            Properties override = new Properties();
            override.load(in);
            PROPS.putAll(override);
            log.info("已加载工作目录下的 config.properties，覆盖默认配置");
        } catch (Exception e) {
            log.warn("读取工作目录配置失败: {}", e.getMessage());
        }
    }

    public static int getResultPageSizeOrDefault(int defaultValue) {
        int fallback = defaultValue;
        if (runtimePageSizeOverride != null) {
            fallback = runtimePageSizeOverride;
        }
        String raw = PROPS.getProperty("result.pageSize");
        int pageSize = parseIntOrDefault(raw, fallback);
        if (pageSize < 1) {
            OperationLog.log("配置 result.pageSize=" + pageSize + " 无效，使用默认值 " + defaultValue);
            pageSize = defaultValue;
        }
        int maxPageSize = getMaxPageSize();
        if (pageSize > maxPageSize) {
            OperationLog.log("配置 result.pageSize=" + pageSize + " 超出上限，已裁剪到 " + maxPageSize);
            pageSize = maxPageSize;
        }
        return pageSize;
    }

    public static int getDefaultPageSize() {
        return getResultPageSizeOrDefault(DEFAULT_PAGE_SIZE);
    }

    public static int getMaxPageSize() {
        String raw = PROPS.getProperty("result.pageSize.max");
        int configured = parseIntOrDefault(raw, DEFAULT_MAX_PAGE_SIZE);
        if (configured < 1) {
            configured = DEFAULT_MAX_PAGE_SIZE;
        }
        return configured;
    }

    public static void overrideDefaultPageSize(int pageSize) {
        int sanitized = pageSize;
        if (sanitized < 1) {
            sanitized = DEFAULT_PAGE_SIZE;
        }
        int max = getMaxPageSize();
        if (sanitized > max) {
            sanitized = max;
        }
        runtimePageSizeOverride = sanitized;
    }

    public static boolean allowPagingAfterFirstFetch() {
        String raw = PROPS.getProperty("allowPagingAfterFirstFetch");
        if (raw == null || raw.isBlank()) {
            return false;
        }
        return raw.trim().equalsIgnoreCase("true");
    }

    private static int parseIntOrDefault(String raw, int defaultValue) {
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("配置项解析失败，使用默认值 {}: {}", defaultValue, raw);
            return defaultValue;
        }
    }
}
