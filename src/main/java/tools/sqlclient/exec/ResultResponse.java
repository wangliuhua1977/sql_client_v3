package tools.sqlclient.exec;

import com.google.gson.JsonArray;

import java.util.List;

public class ResultResponse {
    private String jobId;
    private Boolean success;
    private String status;
    private String code;
    private Boolean resultAvailable;
    private Boolean archived;
    private String archiveError;
    private Long expiresAt;
    private Long lastAccessAt;
    private Boolean overloaded;
    private ThreadPoolSnapshot threadPool;
    private Integer progressPercent;
    private Long durationMillis;
    private Integer rowsAffected;
    private Long queuedAt;
    private Long queueDelayMillis;
    private Integer returnedRowCount;
    private Integer actualRowCount;
    private Integer maxVisibleRows;
    private Integer maxTotalRows;
    private Boolean hasResultSet;
    private Integer page;
    private Integer pageSize;
    private Boolean hasNext;
    private Boolean truncated;
    private String message;
    private String note;
    private List<String> columns;
    private List<java.util.Map<String, String>> rowMaps;
    private JsonArray rawRows;

    public String getJobId() {
        return jobId;
    }

    public Boolean getSuccess() {
        return success;
    }

    public String getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

    public Boolean getResultAvailable() {
        return resultAvailable;
    }

    public Boolean getArchived() {
        return archived;
    }

    public String getArchiveError() {
        return archiveError;
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public Long getLastAccessAt() {
        return lastAccessAt;
    }

    public Boolean getOverloaded() {
        return overloaded;
    }

    public ThreadPoolSnapshot getThreadPool() {
        return threadPool;
    }

    public Integer getProgressPercent() {
        return progressPercent;
    }

    public Long getDurationMillis() {
        return durationMillis;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    public Long getQueuedAt() {
        return queuedAt;
    }

    public Long getQueueDelayMillis() {
        return queueDelayMillis;
    }

    public Integer getReturnedRowCount() {
        return returnedRowCount;
    }

    public Integer getActualRowCount() {
        return actualRowCount;
    }

    public Integer getMaxVisibleRows() {
        return maxVisibleRows;
    }

    public Integer getMaxTotalRows() {
        return maxTotalRows;
    }

    public Boolean getHasResultSet() {
        return hasResultSet;
    }

    public Integer getPage() {
        return page;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public Boolean getHasNext() {
        return hasNext;
    }

    public Boolean getTruncated() {
        return truncated;
    }

    public String getMessage() {
        return message;
    }

    public String getNote() {
        return note;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<java.util.Map<String, String>> getRowMaps() {
        return rowMaps;
    }

    public JsonArray getRawRows() {
        return rawRows;
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

    public void setCode(String code) {
        this.code = code;
    }

    public void setResultAvailable(Boolean resultAvailable) {
        this.resultAvailable = resultAvailable;
    }

    public void setArchived(Boolean archived) {
        this.archived = archived;
    }

    public void setArchiveError(String archiveError) {
        this.archiveError = archiveError;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public void setLastAccessAt(Long lastAccessAt) {
        this.lastAccessAt = lastAccessAt;
    }

    public void setOverloaded(Boolean overloaded) {
        this.overloaded = overloaded;
    }

    public void setThreadPool(ThreadPoolSnapshot threadPool) {
        this.threadPool = threadPool;
    }

    public void setProgressPercent(Integer progressPercent) {
        this.progressPercent = progressPercent;
    }

    public void setDurationMillis(Long durationMillis) {
        this.durationMillis = durationMillis;
    }

    public void setRowsAffected(Integer rowsAffected) {
        this.rowsAffected = rowsAffected;
    }

    public void setQueuedAt(Long queuedAt) {
        this.queuedAt = queuedAt;
    }

    public void setQueueDelayMillis(Long queueDelayMillis) {
        this.queueDelayMillis = queueDelayMillis;
    }

    public void setReturnedRowCount(Integer returnedRowCount) {
        this.returnedRowCount = returnedRowCount;
    }

    public void setActualRowCount(Integer actualRowCount) {
        this.actualRowCount = actualRowCount;
    }

    public void setMaxVisibleRows(Integer maxVisibleRows) {
        this.maxVisibleRows = maxVisibleRows;
    }

    public void setMaxTotalRows(Integer maxTotalRows) {
        this.maxTotalRows = maxTotalRows;
    }

    public void setHasResultSet(Boolean hasResultSet) {
        this.hasResultSet = hasResultSet;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public void setHasNext(Boolean hasNext) {
        this.hasNext = hasNext;
    }

    public void setTruncated(Boolean truncated) {
        this.truncated = truncated;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setRowMaps(List<java.util.Map<String, String>> rowMaps) {
        this.rowMaps = rowMaps;
    }

    public void setRawRows(JsonArray rawRows) {
        this.rawRows = rawRows;
    }
}
