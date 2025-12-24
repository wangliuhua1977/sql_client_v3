package tools.sqlclient.exec;

import java.util.List;
import java.util.Objects;

/**
 * SQL 执行结果模型，包含列名、行数据与原始 SQL，以及异步任务元数据。
 */
public class SqlExecResult {
    private final String sql;
    private final List<String> columns;
    private final List<ColumnDef> columnDefs;
    private final List<List<String>> rows;
    private final List<java.util.Map<String, String>> rowMaps;
    private final int rowsCount;
    private final boolean success;
    private final String message;
    private final String jobId;
    private final String status;
    private final Integer progressPercent;
    private final Long elapsedMillis;
    private final Long durationMillis;
    private final Integer rowsAffected;
    private final Integer returnedRowCount;
    private final Integer actualRowCount;
    private final Integer maxVisibleRows;
    private final Integer maxTotalRows;
    private final Boolean hasResultSet;
    private final Integer page;
    private final Integer pageSize;
    private final Boolean hasNext;
    private final Boolean truncated;
    private final String note;
    private final Long queuedAt;
    private final Long queueDelayMillis;
    private final Boolean overloaded;
    private final ThreadPoolSnapshot threadPool;
    private final String commandTag;
    private final Integer updateCount;
    private final List<String> notices;
    private final List<String> warnings;

    public SqlExecResult(String sql, List<String> columns, List<List<String>> rows, int rowsCount) {
        this(sql, columns, null, rows, null, rowsCount, true, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public SqlExecResult(String sql,
                         List<String> columns,
                         List<ColumnDef> columnDefs,
                         List<List<String>> rows,
                         List<java.util.Map<String, String>> rowMaps,
                         int rowsCount,
                         boolean success,
                         String message,
                         String jobId,
                         String status,
                         Integer progressPercent,
                         Long elapsedMillis,
                         Long durationMillis,
                         Integer rowsAffected,
                         Integer returnedRowCount,
                         Integer actualRowCount,
                         Integer maxVisibleRows,
                         Integer maxTotalRows,
                         Boolean hasResultSet,
                         Integer page,
                         Integer pageSize,
                         Boolean hasNext,
                         Boolean truncated,
                         String note,
                         Long queuedAt,
                         Long queueDelayMillis,
                         Boolean overloaded,
                         ThreadPoolSnapshot threadPool,
                         String commandTag,
                         Integer updateCount,
                         List<String> notices,
                         List<String> warnings) {
        this.sql = Objects.requireNonNullElse(sql, "");
        this.columns = columns;
        this.columnDefs = columnDefs;
        this.rows = rows;
        this.rowMaps = rowMaps;
        this.rowsCount = rowsCount;
        this.success = success;
        this.message = message;
        this.jobId = jobId;
        this.status = status;
        this.progressPercent = progressPercent;
        this.elapsedMillis = elapsedMillis;
        this.durationMillis = durationMillis;
        this.rowsAffected = rowsAffected;
        this.returnedRowCount = returnedRowCount;
        this.actualRowCount = actualRowCount;
        this.maxVisibleRows = maxVisibleRows;
        this.maxTotalRows = maxTotalRows;
        this.hasResultSet = hasResultSet;
        this.page = page;
        this.pageSize = pageSize;
        this.hasNext = hasNext;
        this.truncated = truncated;
        this.note = note;
        this.queuedAt = queuedAt;
        this.queueDelayMillis = queueDelayMillis;
        this.overloaded = overloaded;
        this.threadPool = threadPool;
        this.commandTag = commandTag;
        this.updateCount = updateCount;
        this.notices = notices;
        this.warnings = warnings;
    }

    public SqlExecResult(String sql,
                         List<String> columns,
                         List<ColumnDef> columnDefs,
                         List<List<String>> rows,
                         List<java.util.Map<String, String>> rowMaps,
                         int rowsCount,
                         boolean success,
                         String message,
                         String jobId,
                         String status,
                         Integer progressPercent,
                         Long elapsedMillis,
                         Long durationMillis,
                         Integer rowsAffected,
                         Integer returnedRowCount,
                         Integer actualRowCount,
                         Integer maxVisibleRows,
                         Integer maxTotalRows,
                         Boolean hasResultSet,
                         Integer page,
                         Integer pageSize,
                         Boolean hasNext,
                         Boolean truncated,
                         String note) {
        this(sql, columns, columnDefs, rows, rowMaps, rowsCount, success, message, jobId, status, progressPercent,
                elapsedMillis, durationMillis, rowsAffected, returnedRowCount, actualRowCount, maxVisibleRows, maxTotalRows,
                hasResultSet, page, pageSize, hasNext, truncated, note, null, null, null, null,
                null, null, null, null);
    }

    public String getSql() {
        return sql;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    public List<java.util.Map<String, String>> getRowMaps() {
        return rowMaps;
    }

    public int getRowsCount() {
        return rowsCount;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public String getJobId() {
        return jobId;
    }

    public String getStatus() {
        return status;
    }

    public Integer getProgressPercent() {
        return progressPercent;
    }

    public Long getElapsedMillis() {
        return elapsedMillis;
    }

    public Long getDurationMillis() {
        return durationMillis;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    public Integer getReturnedRowCount() {
        return returnedRowCount;
    }

    public Integer getActualRowCount() {
        return actualRowCount;
    }

    public Integer getMaxVisibleRows() {
        return maxVisibleRows;
    }

    public Integer getMaxTotalRows() {
        return maxTotalRows;
    }

    public Boolean getHasResultSet() {
        return hasResultSet;
    }

    public Integer getPage() {
        return page;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public Boolean getHasNext() {
        return hasNext;
    }

    public Boolean getTruncated() {
        return truncated;
    }

    public String getNote() {
        return note;
    }

    public Long getQueuedAt() {
        return queuedAt;
    }

    public Long getQueueDelayMillis() {
        return queueDelayMillis;
    }

    public Boolean getOverloaded() {
        return overloaded;
    }

    public ThreadPoolSnapshot getThreadPool() {
        return threadPool;
    }

    public String getCommandTag() {
        return commandTag;
    }

    public Integer getUpdateCount() {
        return updateCount;
    }

    public List<String> getNotices() {
        return notices;
    }

    public List<String> getWarnings() {
        return warnings;
    }
}
