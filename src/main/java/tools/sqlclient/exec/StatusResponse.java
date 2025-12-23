package tools.sqlclient.exec;

public class StatusResponse {
    private String jobId;
    private Boolean success;
    private String status;
    private Integer progressPercent;
    private Long elapsedMillis;
    private Boolean overloaded;
    private Long queuedAt;
    private Long queueDelayMillis;
    private ThreadPoolSnapshot threadPool;
    private String message;

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

    public Boolean getOverloaded() {
        return overloaded;
    }

    public Long getQueuedAt() {
        return queuedAt;
    }

    public Long getQueueDelayMillis() {
        return queueDelayMillis;
    }

    public ThreadPoolSnapshot getThreadPool() {
        return threadPool;
    }

    public String getMessage() {
        return message;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setProgressPercent(Integer progressPercent) {
        this.progressPercent = progressPercent;
    }

    public void setElapsedMillis(Long elapsedMillis) {
        this.elapsedMillis = elapsedMillis;
    }

    public void setOverloaded(Boolean overloaded) {
        this.overloaded = overloaded;
    }

    public void setQueuedAt(Long queuedAt) {
        this.queuedAt = queuedAt;
    }

    public void setQueueDelayMillis(Long queueDelayMillis) {
        this.queueDelayMillis = queueDelayMillis;
    }

    public void setThreadPool(ThreadPoolSnapshot threadPool) {
        this.threadPool = threadPool;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
