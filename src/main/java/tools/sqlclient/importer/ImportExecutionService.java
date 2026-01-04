package tools.sqlclient.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.exec.SqlExecutionService;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 多线程导入执行：Reader + Writer。
 */
public class ImportExecutionService {
    private static final Logger log = LoggerFactory.getLogger(ImportExecutionService.class);
    private static final int MAX_BATCH_ROWS = 300;
    private final SqlExecutionService sqlExecutionService = new SqlExecutionService();
    private final PgSqlBuilder sqlBuilder = new PgSqlBuilder();

    public void execute(RowSource source,
                        List<ColumnSpec> columns,
                        String schema,
                        String table,
                        Mode mode,
                        List<String> dedupKeys,
                        Consumer<String> logAppender,
                        Consumer<ImportReport> onFinish,
                        AtomicBoolean cancelFlag) {
        ArrayBlockingQueue<RowData> queue = new ArrayBlockingQueue<>(2000);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ImportReport report = new ImportReport();
        report.setSourceType(mode.name());
        report.setColumns(columns);
        report.setTargetTable(schema + "." + table);
        report.setStart(Instant.now());
        AtomicLong produced = new AtomicLong();
        AtomicLong inserted = new AtomicLong();
        AtomicLong skipped = new AtomicLong();
        AtomicLong failed = new AtomicLong();

        Runnable producer = () -> {
            try {
                source.open();
                RowData row;
                while (!cancelFlag.get() && (row = source.nextRow()) != null) {
                    queue.put(row);
                    produced.incrementAndGet();
                }
            } catch (IOException | InterruptedException e) {
                cancelFlag.set(true);
                report.setErrorMessage(e.getMessage());
            } finally {
                try {
                    queue.put(new RowData(-1, List.of()));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                try {
                    source.close();
                } catch (IOException ignored) {
                }
            }
        };

        Runnable consumer = () -> {
            try {
                prepareTable(mode, schema, table, columns, logAppender);
                List<List<String>> currentBatch = new ArrayList<>();
                RowData row;
                while (!cancelFlag.get() && (row = queue.take()).getLineNumber() != -1) {
                    currentBatch.add(row.getValues());
                    if (currentBatch.size() >= MAX_BATCH_ROWS) {
                        flushBatch(schema, table, columns, currentBatch, dedupKeys, inserted, skipped, logAppender);
                        currentBatch.clear();
                    }
                }
                if (!currentBatch.isEmpty() && !cancelFlag.get()) {
                    flushBatch(schema, table, columns, currentBatch, dedupKeys, inserted, skipped, logAppender);
                }
            } catch (Exception e) {
                cancelFlag.set(true);
                failed.incrementAndGet();
                report.setErrorMessage(e.getMessage());
            }
        };

        executor.submit(producer);
        executor.submit(consumer);
        executor.shutdown();
        new Thread(() -> {
            while (!executor.isTerminated()) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            report.setEnd(Instant.now());
            report.setSourceRowCount(produced.get());
            report.setInsertedRows(inserted.get());
            report.setSkippedRows(skipped.get());
            report.setFailedRows(failed.get());
            report.setBatchCount((int) Math.ceil((double) inserted.get() / Math.max(1, MAX_BATCH_ROWS)));
            onFinish.accept(report);
        }, "import-waiter").start();
    }

    private void prepareTable(Mode mode, String schema, String table, List<ColumnSpec> columns, Consumer<String> logAppender) {
        if (logAppender != null) {
            logAppender.accept("准备表: " + mode);
        }
        if (mode == Mode.NEW_TABLE) {
            sqlExecutionService.executeSync(sqlBuilder.buildCreateTable(schema, table, columns));
        } else if (mode == Mode.TRUNCATE_INSERT) {
            sqlExecutionService.executeSync(sqlBuilder.buildTruncate(schema, table));
        }
    }

    private void flushBatch(String schema,
                            String table,
                            List<ColumnSpec> columns,
                            List<List<String>> batch,
                            List<String> dedupKeys,
                            AtomicLong inserted,
                            AtomicLong skipped,
                            Consumer<String> logAppender) {
        String sql = sqlBuilder.buildInsert(schema, table, columns, batch, dedupKeys);
        if (logAppender != null) {
            logAppender.accept("提交批次，行数: " + batch.size());
        }
        try {
            sqlExecutionService.executeSync(sql);
            inserted.addAndGet(batch.size());
        } catch (Exception e) {
            skipped.addAndGet(batch.size());
            throw e;
        }
    }

    public enum Mode {
        NEW_TABLE,
        TRUNCATE_INSERT,
        APPEND_DEDUP
    }
}
