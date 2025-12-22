package tools.sqlclient.exec;

/**
 * 异步任务状态模型，供 UI 展示进度。
 */
public class AsyncJobStatus {
    private final String jobId;
    private final Boolean success;
    private final String status;
    private final Integer progressPercent;
    private final Long elapsedMillis;
    private final String label;
    private final String sqlSummary;
    private final Integer rowsAffected;
    private final Integer returnedRowCount;
    private final Boolean hasResultSet;
    private final Integer actualRowCount;
    private final String message;
    private final Long queuedAt;
    private final Long queueDelayMillis;
    private final Boolean overloaded;
    private final ThreadPoolSnapshot threadPool;

    public AsyncJobStatus(String jobId,
                          Boolean success,
                          String status,
                          Integer progressPercent,
                          Long elapsedMillis,
                          String label,
                          String sqlSummary,
                          Integer rowsAffected,
                          Integer returnedRowCount,
                          Boolean hasResultSet,
                          Integer actualRowCount,
                          String message,
                          Long queuedAt,
                          Long queueDelayMillis,
                          Boolean overloaded,
                          ThreadPoolSnapshot threadPool) {
        this.jobId = jobId;
        this.success = success;
        this.status = status;
        this.progressPercent = progressPercent;
        this.elapsedMillis = elapsedMillis;
        this.label = label;
        this.sqlSummary = sqlSummary;
        this.rowsAffected = rowsAffected;
        this.returnedRowCount = returnedRowCount;
        this.hasResultSet = hasResultSet;
        this.actualRowCount = actualRowCount;
        this.message = message;
        this.queuedAt = queuedAt;
        this.queueDelayMillis = queueDelayMillis;
        this.overloaded = overloaded;
        this.threadPool = threadPool;
    }

    public String getJobId() {
        return jobId;
    }

    public Boolean getSuccess() {
        return success;
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

    public String getLabel() {
        return label;
    }

    public String getSqlSummary() {
        return sqlSummary;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    public Integer getReturnedRowCount() {
        return returnedRowCount;
    }

    public Boolean getHasResultSet() {
        return hasResultSet;
    }

    public Integer getActualRowCount() {
        return actualRowCount;
    }

    public String getMessage() {
        return message;
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
}
