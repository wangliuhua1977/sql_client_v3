package tools.sqlclient.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * 自动保存服务，后台线程定期调用保存逻辑。
 */
public class AutoSaveService {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "auto-save"));
    private final Consumer<String> autosaveCallback;
    private final IntConsumer taskCountCallback;
    private final AtomicInteger runningTasks = new AtomicInteger(0);

    public AutoSaveService(Consumer<String> autosaveCallback, IntConsumer taskCountCallback) {
        this.autosaveCallback = autosaveCallback;
        this.taskCountCallback = taskCountCallback;
    }

    public void startAutoSave(Runnable saveTask) {
        scheduler.scheduleWithFixedDelay(() -> {
            runningTasks.incrementAndGet();
            updateTaskCount();
            try {
                saveTask.run();
            } finally {
                runningTasks.decrementAndGet();
                updateTaskCount();
            }
        }, 5, 30, TimeUnit.SECONDS);
    }

    public void onSaved(String time) {
        autosaveCallback.accept(time);
    }

    private void updateTaskCount() {
        taskCountCallback.accept(runningTasks.get());
    }
}
