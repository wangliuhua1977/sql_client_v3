package tools.sqlclient.exec;

/**
 * 标识结果暂不可用（例如短暂过期或分页基准差异），可通过重试或重提任务恢复。
 */
public class TransientResultUnavailableException extends RuntimeException {
    private final String jobId;

    public TransientResultUnavailableException(String jobId, String message) {
        super(message);
        this.jobId = jobId;
    }

    public TransientResultUnavailableException(String jobId, String message, Throwable cause) {
        super(message, cause);
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }
}
