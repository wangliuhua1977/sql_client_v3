package tools.sqlclient.importer;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImportExecutionService {
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final ArrayBlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(2000);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public void start(RowSource source, ImportReport report) {
        report.markStart();
        executor.submit(() -> produce(source));
        executor.submit(() -> consume(report));
    }

    private void produce(RowSource source) {
        try (RowSource ignored = source) {
            for (List<String> row : source) {
                if (cancelled.get()) {
                    break;
                }
                queue.put(row);
            }
        } catch (Exception e) {
            cancelled.set(true);
        } finally {
            queue.offer(List.of());
        }
    }

    private void consume(ImportReport report) {
        try {
            while (true) {
                List<String> row = queue.take();
                if (row.isEmpty()) {
                    break;
                }
                report.setSourceRowCount(report.getSourceRowCount() + 1);
                report.setInsertedRowCount(report.getInsertedRowCount() + 1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancelled.set(true);
        } finally {
            report.markEnd();
        }
    }

    public void cancel() {
        cancelled.set(true);
        executor.shutdownNow();
    }

    public void awaitTermination() {
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
