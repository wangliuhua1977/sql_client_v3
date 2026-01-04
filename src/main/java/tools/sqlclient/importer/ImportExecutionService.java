package tools.sqlclient.importer;

import tools.sqlclient.util.OperationLog;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Producer-consumer pipeline to stream rows and simulate batch import.
 */
public class ImportExecutionService {
    private static final int DEFAULT_QUEUE_CAPACITY = 2000;
    private static final int MAX_BATCH_ROWS = 300;

    public record ImportOptions(ImportReport.Mode mode, String tableName) {
    }

    public Future<?> execute(RowSource source,
                             ImportOptions options,
                             Consumer<String> logConsumer,
                             Consumer<ImportReport> doneConsumer,
                             AtomicBoolean cancelled) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");
        BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ImportReport report = new ImportReport();
        report.setStartTime(Instant.now());
        report.setMode(options.mode());
        report.setTableName(options.tableName());
        report.setHeader(source.getHeader());
        AtomicLong batchCounter = new AtomicLong();
        AtomicLong sourceCount = new AtomicLong();
        AtomicLong inserted = new AtomicLong();
        AtomicLong failed = new AtomicLong();

        Runnable reader = () -> {
            try {
                List<String> row;
                while (!cancelled.get() && (row = source.nextRow()) != null) {
                    queue.put(row);
                    sourceCount.incrementAndGet();
                }
            } catch (Exception e) {
                failed.incrementAndGet();
                report.setErrorMessage(e.getMessage());
                logConsumer.accept("读取失败: " + e.getMessage());
            } finally {
                try {
                    queue.put(List.of()); // poison
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        Runnable writer = () -> {
            try {
                List<String> row;
                List<List<String>> batch = new java.util.ArrayList<>();
                while (true) {
                    row = queue.poll(500, TimeUnit.MILLISECONDS);
                    if (row == null) {
                        if (cancelled.get()) break;
                        else continue;
                    }
                    if (row.isEmpty()) {
                        flushBatch(batch, inserted, batchCounter, logConsumer);
                        break;
                    }
                    batch.add(row);
                    if (batch.size() >= MAX_BATCH_ROWS) {
                        flushBatch(batch, inserted, batchCounter, logConsumer);
                        batch = new java.util.ArrayList<>();
                    }
                    if (cancelled.get()) {
                        break;
                    }
                }
            } catch (Exception e) {
                report.setErrorMessage(e.getMessage());
                failed.incrementAndGet();
                logConsumer.accept("写入失败: " + e.getMessage());
            }
        };

        Future<?> readerFuture = executor.submit(reader);
        Future<?> writerFuture = executor.submit(writer);

        return executor.submit(() -> {
            try {
                readerFuture.get();
                writerFuture.get();
            } catch (Exception e) {
                report.setErrorMessage(e.getMessage());
            } finally {
                executor.shutdownNow();
                report.setEndTime(Instant.now());
                report.setSourceRows(sourceCount.get());
                report.setInsertedRows(inserted.get());
                report.setFailedRows(failed.get());
                report.setBatchCount(batchCounter.get());
                report.setCancelled(cancelled.get());
                try {
                    source.close();
                } catch (Exception ignored) {
                    OperationLog.log("关闭 source 失败: " + ignored.getMessage());
                }
                doneConsumer.accept(report);
            }
        });
    }

    private void flushBatch(List<List<String>> batch,
                            AtomicLong inserted,
                            AtomicLong batchCounter,
                            Consumer<String> logConsumer) {
        if (batch.isEmpty()) {
            return;
        }
        batchCounter.incrementAndGet();
        inserted.addAndGet(batch.size());
        logConsumer.accept("批次写入 " + batch.size() + " 行");
    }
}
