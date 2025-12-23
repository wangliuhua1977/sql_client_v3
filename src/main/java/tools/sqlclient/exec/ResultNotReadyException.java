package tools.sqlclient.exec;

public class ResultNotReadyException extends RuntimeException {
    private final String jobId;

    public ResultNotReadyException(String jobId, String message) {
        super(message);
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }
}
