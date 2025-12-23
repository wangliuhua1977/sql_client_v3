package tools.sqlclient.exec;

public class ResultExpiredException extends RuntimeException {
    private final String jobId;

    public ResultExpiredException(String jobId, String message) {
        super(message);
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }
}
