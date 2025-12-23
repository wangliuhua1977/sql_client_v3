package tools.sqlclient.exec;

public class ResultExpiredException extends RuntimeException {
    private final String jobId;
    private final Long expiresAt;
    private final Long lastAccessAt;

    public ResultExpiredException(String jobId, String message, Long expiresAt, Long lastAccessAt) {
        super(message);
        this.jobId = jobId;
        this.expiresAt = expiresAt;
        this.lastAccessAt = lastAccessAt;
    }

    public String getJobId() {
        return jobId;
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public Long getLastAccessAt() {
        return lastAccessAt;
    }
}
