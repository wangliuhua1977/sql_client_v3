package tools.sqlclient.exec;

/**
 * 异步任务状态模型，供 UI 展示进度。
 */
public class AsyncJobStatus {
    private final String jobId;
    private final String status;
    private final Integer progressPercent;
    private final Long elapsedMillis;
    private final String label;
    private final String sqlSummary;
    private final Integer rowsAffected;
    private final Integer returnedRowCount;
    private final Boolean hasResultSet;
    private final String message;

    public AsyncJobStatus(String jobId,
                          String status,
                          Integer progressPercent,
                          Long elapsedMillis,
                          String label,
                          String sqlSummary,
                          Integer rowsAffected,
                          Integer returnedRowCount,
                          Boolean hasResultSet,
                          String message) {
        this.jobId = jobId;
        this.status = status;
        this.progressPercent = progressPercent;
        this.elapsedMillis = elapsedMillis;
        this.label = label;
        this.sqlSummary = sqlSummary;
        this.rowsAffected = rowsAffected;
        this.returnedRowCount = returnedRowCount;
        this.hasResultSet = hasResultSet;
        this.message = message;
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

    public String getMessage() {
        return message;
    }
}
