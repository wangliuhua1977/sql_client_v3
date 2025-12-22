package tools.sqlclient.exec;

/**
 * 后端线程池监控摘要，便于前端日志输出与可观测。
 */
public class ThreadPoolSnapshot {
    private final Integer poolSize;
    private final Integer activeCount;
    private final Integer queueSize;
    private final Long taskCount;
    private final Long completedTaskCount;
    private final Integer jobStoreSize;

    public ThreadPoolSnapshot(Integer poolSize,
                              Integer activeCount,
                              Integer queueSize,
                              Long taskCount,
                              Long completedTaskCount,
                              Integer jobStoreSize) {
        this.poolSize = poolSize;
        this.activeCount = activeCount;
        this.queueSize = queueSize;
        this.taskCount = taskCount;
        this.completedTaskCount = completedTaskCount;
        this.jobStoreSize = jobStoreSize;
    }

    public Integer getPoolSize() {
        return poolSize;
    }

    public Integer getActiveCount() {
        return activeCount;
    }

    public Integer getQueueSize() {
        return queueSize;
    }

    public Long getTaskCount() {
        return taskCount;
    }

    public Long getCompletedTaskCount() {
        return completedTaskCount;
    }

    public Integer getJobStoreSize() {
        return jobStoreSize;
    }

    @Override
    public String toString() {
        return "pool=" + poolSize
                + " active=" + activeCount
                + " queue=" + queueSize
                + " task=" + taskCount
                + " done=" + completedTaskCount
                + " store=" + jobStoreSize;
    }
}
