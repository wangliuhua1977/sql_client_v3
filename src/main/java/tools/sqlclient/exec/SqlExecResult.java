package tools.sqlclient.exec;

import java.util.List;
import java.util.Objects;

/**
 * SQL 执行结果模型，包含列名、行数据与原始 SQL，以及异步任务元数据。
 */
public class SqlExecResult {
    private final String sql;
    private final List<String> columns;
    private final List<List<String>> rows;
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

    public SqlExecResult(String sql, List<String> columns, List<List<String>> rows, int rowsCount) {
        this(sql, columns, rows, rowsCount, true, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null);
    }

    public SqlExecResult(String sql,
                         List<String> columns,
                         List<List<String>> rows,
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
        this.sql = Objects.requireNonNullElse(sql, "");
        this.columns = columns;
        this.rows = rows;
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
}
