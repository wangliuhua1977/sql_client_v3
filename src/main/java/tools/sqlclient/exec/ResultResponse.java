package tools.sqlclient.exec;

import com.google.gson.JsonArray;

import java.util.List;
import java.util.Map;

public class ResultResponse {
    private String jobId;
    private Boolean success;
    private String status;
    private String code;
    private Long submittedAt;
    private Boolean resultAvailable;
    private Boolean archived;
    private String archiveStatus;
    private String archiveError;
    private Long archivedAt;
    private Long expiresAt;
    private Long lastAccessAt;
    private Long startedAt;
    private Long finishedAt;
    private String dbUser;
    private String label;
    private String sqlSummary;
    private Boolean overloaded;
    private ThreadPoolSnapshot threadPool;
    private Integer progressPercent;
    private Long durationMillis;
    private Integer rowsAffected;
    private Long queuedAt;
    private Long queueDelayMillis;
    private Integer offset;
    private Integer limit;
    private Integer returnedRowCount;
    private Integer actualRowCount;
    private Integer maxVisibleRows;
    private Integer maxTotalRows;
    private Boolean hasResultSet;
    private Boolean isSelect;
    private String resultType;
    private Integer updateCount;
    private String commandTag;
    private Integer page;
    private Integer pageSize;
    private Boolean hasNext;
    private Boolean truncated;
    private DatabaseErrorInfo error;
    private String message;
    private String errorMessage;
    private Integer position;
    private String note;
    private List<String> notices;
    private List<String> warnings;
    private List<String> columns;
    private List<String> columnExpressions;
    private List<ColumnMeta> columnMetas;
    private List<java.util.Map<String, String>> rowMaps;
    private List<Map<String, Object>> resultRows;
    private JsonArray rawRows;
    private Integer totalRows;

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

    public Long getSubmittedAt() {
        return submittedAt;
    }

    public Boolean getResultAvailable() {
        return resultAvailable;
    }

    public Boolean getArchived() {
        return archived;
    }

    public String getArchiveStatus() {
        return archiveStatus;
    }

    public String getArchiveError() {
        return archiveError;
    }

    public Long getArchivedAt() {
        return archivedAt;
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public Long getLastAccessAt() {
        return lastAccessAt;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public Long getFinishedAt() {
        return finishedAt;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getLabel() {
        return label;
    }

    public String getSqlSummary() {
        return sqlSummary;
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

    public Integer getOffset() {
        return offset;
    }

    public Integer getLimit() {
        return limit;
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

    public Boolean getIsSelect() {
        return isSelect;
    }

    public String getResultType() {
        return resultType;
    }

    public Integer getUpdateCount() {
        return updateCount;
    }

    public String getCommandTag() {
        return commandTag;
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

    public DatabaseErrorInfo getError() {
        return error;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Integer getPosition() {
        return position;
    }

    public String getNote() {
        return note;
    }

    public List<String> getNotices() {
        return notices;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getColumnExpressions() {
        return columnExpressions;
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public List<java.util.Map<String, String>> getRowMaps() {
        return rowMaps;
    }

    public List<Map<String, Object>> getResultRows() {
        return resultRows;
    }

    public JsonArray getRawRows() {
        return rawRows;
    }

    public Integer getTotalRows() {
        return totalRows;
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

    public void setSubmittedAt(Long submittedAt) {
        this.submittedAt = submittedAt;
    }

    public void setResultAvailable(Boolean resultAvailable) {
        this.resultAvailable = resultAvailable;
    }

    public void setArchived(Boolean archived) {
        this.archived = archived;
    }

    public void setArchiveStatus(String archiveStatus) {
        this.archiveStatus = archiveStatus;
    }

    public void setArchiveError(String archiveError) {
        this.archiveError = archiveError;
    }

    public void setArchivedAt(Long archivedAt) {
        this.archivedAt = archivedAt;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public void setLastAccessAt(Long lastAccessAt) {
        this.lastAccessAt = lastAccessAt;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public void setFinishedAt(Long finishedAt) {
        this.finishedAt = finishedAt;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setSqlSummary(String sqlSummary) {
        this.sqlSummary = sqlSummary;
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

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
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

    public void setIsSelect(Boolean isSelect) {
        this.isSelect = isSelect;
    }

    public void setResultType(String resultType) {
        this.resultType = resultType;
    }

    public void setUpdateCount(Integer updateCount) {
        this.updateCount = updateCount;
    }

    public void setCommandTag(String commandTag) {
        this.commandTag = commandTag;
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

    public void setError(DatabaseErrorInfo error) {
        this.error = error;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public void setNotices(List<String> notices) {
        this.notices = notices;
    }

    public void setWarnings(List<String> warnings) {
        this.warnings = warnings;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setColumnExpressions(List<String> columnExpressions) {
        this.columnExpressions = columnExpressions;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public void setRowMaps(List<java.util.Map<String, String>> rowMaps) {
        this.rowMaps = rowMaps;
    }

    public void setResultRows(List<Map<String, Object>> resultRows) {
        this.resultRows = resultRows;
    }

    public void setRawRows(JsonArray rawRows) {
        this.rawRows = rawRows;
    }

    public void setTotalRows(Integer totalRows) {
        this.totalRows = totalRows;
    }
}
