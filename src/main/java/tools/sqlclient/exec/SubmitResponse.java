package tools.sqlclient.exec;

public class SubmitResponse {
    private String jobId;
    private Boolean success;
    private Boolean overloaded;
    private String message;
    private ThreadPoolSnapshot threadPool;
    private Long queuedAt;
    private Long queueDelayMillis;
    private String code;

    public String getJobId() {
        return jobId;
    }

    public Boolean getSuccess() {
        return success;
    }

    public Boolean getOverloaded() {
        return overloaded;
    }

    public String getMessage() {
        return message;
    }

    public ThreadPoolSnapshot getThreadPool() {
        return threadPool;
    }

    public Long getQueuedAt() {
        return queuedAt;
    }

    public Long getQueueDelayMillis() {
        return queueDelayMillis;
    }

    public String getCode() {
        return code;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public void setOverloaded(Boolean overloaded) {
        this.overloaded = overloaded;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setThreadPool(ThreadPoolSnapshot threadPool) {
        this.threadPool = threadPool;
    }

    public void setQueuedAt(Long queuedAt) {
        this.queuedAt = queuedAt;
    }

    public void setQueueDelayMillis(Long queueDelayMillis) {
        this.queueDelayMillis = queueDelayMillis;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
