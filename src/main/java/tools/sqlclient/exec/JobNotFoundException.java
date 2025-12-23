package tools.sqlclient.exec;

/**
 * 后端返回 HTTP 404 且 message=Job not found 时抛出的业务异常。
 */
public class JobNotFoundException extends RuntimeException {
    private final String jobId;

    public JobNotFoundException(String jobId, String message) {
        super(message);
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }
}
