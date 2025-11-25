package tools.sqlclient.util;

import javax.swing.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

/**
 * UI 内部可复用的操作日志记录器：
 * - 通过 {@link #setAppender(Consumer)} 注入 UI 侧的日志追加方法。
 * - 所有线程均可调用 {@link #log(String)}，内部会在 EDT 上安全追加。
 */
public final class OperationLog {
    private static volatile Consumer<String> appender = null;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private OperationLog() {}

    public static void setAppender(Consumer<String> appender) {
        OperationLog.appender = appender;
    }

    /**
     * 当前是否已经有 UI 端注册了日志追加器。用来在非 UI 线程快速判断是否需要尝试记录，
     * 也避免在类缺失或未初始化时出现空指针，确保编译与运行时的健壮性。
     */
    public static boolean isReady() {
        return appender != null;
    }

    public static void log(String message) {
        Consumer<String> target = appender;
        if (target == null || message == null) {
            return;
        }
        String line = "[" + LocalDateTime.now().format(FORMATTER) + "] " + message;
        if (SwingUtilities.isEventDispatchThread()) {
            target.accept(line);
        } else {
            SwingUtilities.invokeLater(() -> target.accept(line));
        }
    }

    /**
     * 将长文本缩略到指定长度，避免日志面板被超长响应撑满。
     */
    public static String abbreviate(String text, int max) {
        if (text == null) return "<null>";
        if (text.length() <= max) return text;
        return text.substring(0, Math.max(0, max - 3)) + "...";
    }
}
