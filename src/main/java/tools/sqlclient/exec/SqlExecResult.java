package tools.sqlclient.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SQL 执行结果模型，包含列名、行数据与原始 SQL，以及异步任务元数据。
 */
public class SqlExecResult {
    private final String sql;
    private final List<String> columns;
    private final List<ColumnDef> columnDefs;
    private final List<List<String>> rows;
    private final List<java.util.Map<String, String>> rowMaps;
    private final int rowsCount;
    private final boolean success;
    private final String message;
    private final String jobId;
    private final String status;
    private final Integer progressPercent;
    private final Long elapsedMillis;
    private final Long durationMillis;
    private final Integer rowsAffected;
    private final Integer returnedRowCount;
    private final Integer actualRowCount;
    private final Integer maxVisibleRows;
    private final Integer maxTotalRows;
    private final Integer totalRows;
    private final Boolean hasResultSet;
    private final Integer page;
    private final Integer pageSize;
    private final Boolean hasNext;
    private final Boolean truncated;
    private final String note;
    private final Long queuedAt;
    private final Long queueDelayMillis;
    private final Boolean overloaded;
    private final ThreadPoolSnapshot threadPool;
    private final String commandTag;
    private final Integer updateCount;
    private final List<String> notices;
    private final List<String> warnings;

    /**
     * 仅用于兼容旧代码的 4 参构造器，内部委托 Builder，后续请统一使用 {@link #builder(String)}。
     */
    public SqlExecResult(String sql, List<String> columns, List<List<String>> rows, int rowsCount) {
        this(builder(sql).columns(columns).rows(rows).rowCount(rowsCount));
    }

    /**
     * 仅用于兼容旧代码的中间构造器，禁止在业务代码中继续调用，统一改用 Builder。
     */
    @Deprecated
    public SqlExecResult(String sql,
                         List<String> columns,
                         List<ColumnDef> columnDefs,
                         List<List<String>> rows,
                         List<java.util.Map<String, String>> rowMaps,
                         int rowsCount,
                         boolean success,
                         String message,
                         String jobId,
                         String status,
                         Integer progressPercent,
                         Long elapsedMillis,
                         Long durationMillis,
                         Integer rowsAffected,
                         Integer returnedRowCount,
                         Integer actualRowCount,
                         Integer maxVisibleRows,
                         Integer maxTotalRows,
                         Integer totalRows,
                         Boolean hasResultSet,
                         Integer page,
                         Integer pageSize,
                         Boolean hasNext,
                         Boolean truncated,
                         String note,
                         Long queuedAt,
                         Long queueDelayMillis,
                         Boolean overloaded,
                         ThreadPoolSnapshot threadPool,
                         String commandTag,
                         Integer updateCount,
                         List<String> notices,
                         List<String> warnings) {
        this(builder(sql)
                .columns(columns)
                .columnDefs(columnDefs)
                .rows(rows)
                .rowMaps(rowMaps)
                .rowCount(rowsCount)
                .success(success)
                .message(message)
                .jobId(jobId)
                .status(status)
                .progressPercent(progressPercent)
                .elapsedMillis(elapsedMillis)
                .durationMillis(durationMillis)
                .rowsAffected(rowsAffected)
                .returnedRowCount(returnedRowCount)
                .actualRowCount(actualRowCount)
                .maxVisibleRows(maxVisibleRows)
                .maxTotalRows(maxTotalRows)
                .hasResultSet(hasResultSet)
                .page(page)
                .pageSize(pageSize)
                .hasNext(hasNext)
                .truncated(truncated)
                .note(note)
                .queuedAt(queuedAt)
                .queueDelayMillis(queueDelayMillis)
                .overloaded(overloaded)
                .threadPool(threadPool)
                .commandTag(commandTag)
                .updateCount(updateCount)
                .notices(notices)
                .warnings(warnings));
    }

    /**
     * 长参数构造器仅供 Builder 调用，外部禁止直接使用。
     */
    @Deprecated
    public SqlExecResult(String sql,
                         List<String> columns,
                         List<ColumnDef> columnDefs,
                         List<List<String>> rows,
                         List<java.util.Map<String, String>> rowMaps,
                         int rowsCount,
                         boolean success,
                         String message,
                         String jobId,
                         String status,
                         Integer progressPercent,
                         Long elapsedMillis,
                         Long durationMillis,
                         Integer rowsAffected,
                         Integer returnedRowCount,
                         Integer actualRowCount,
                         Integer maxVisibleRows,
                         Integer maxTotalRows,
                         Integer totalRows,
                         Boolean hasResultSet,
                         Integer page,
                         Integer pageSize,
                         Boolean hasNext,
                         Boolean truncated,
                         String note) {
        this(builder(sql)
                .columns(columns)
                .columnDefs(columnDefs)
                .rows(rows)
                .rowMaps(rowMaps)
                .rowCount(rowsCount)
                .success(success)
                .message(message)
                .jobId(jobId)
                .status(status)
                .progressPercent(progressPercent)
                .elapsedMillis(elapsedMillis)
                .durationMillis(durationMillis)
                .rowsAffected(rowsAffected)
                .returnedRowCount(returnedRowCount)
                .actualRowCount(actualRowCount)
                .maxVisibleRows(maxVisibleRows)
                .maxTotalRows(maxTotalRows)
                .totalRows(totalRows)
                .hasResultSet(hasResultSet)
                .page(page)
                .pageSize(pageSize)
                .hasNext(hasNext)
                .truncated(truncated)
                .note(note));
    }

    private SqlExecResult(Builder builder) {
        this.sql = Objects.requireNonNullElse(builder.sql, "");
        this.columns = defaultList(builder.columns);
        this.columnDefs = defaultList(builder.columnDefs);
        this.rows = defaultList(builder.rows);
        this.rowMaps = defaultList(builder.rowMaps);
        this.rowsCount = builder.rowsCount != null
                ? builder.rowsCount
                : (builder.totalRows != null ? builder.totalRows : deriveRowCount());
        this.success = builder.success != null ? builder.success : true;
        this.message = builder.message;
        this.jobId = builder.jobId;
        this.status = builder.status;
        this.progressPercent = builder.progressPercent;
        this.elapsedMillis = builder.elapsedMillis;
        this.durationMillis = builder.durationMillis;
        this.rowsAffected = builder.rowsAffected;
        this.returnedRowCount = builder.returnedRowCount;
        this.actualRowCount = builder.actualRowCount;
        this.maxVisibleRows = builder.maxVisibleRows;
        this.maxTotalRows = builder.maxTotalRows;
        this.totalRows = builder.totalRows != null ? builder.totalRows : builder.rowsCount;
        this.hasResultSet = builder.hasResultSet != null ? builder.hasResultSet : guessHasResultSet();
        this.page = builder.page;
        this.pageSize = builder.pageSize;
        this.hasNext = builder.hasNext;
        this.truncated = builder.truncated;
        this.note = builder.note;
        this.queuedAt = builder.queuedAt;
        this.queueDelayMillis = builder.queueDelayMillis;
        this.overloaded = builder.overloaded;
        this.threadPool = builder.threadPool;
        this.commandTag = builder.commandTag;
        this.updateCount = builder.updateCount;
        this.notices = defaultList(builder.notices);
        this.warnings = defaultList(builder.warnings);
    }

    private int deriveRowCount() {
        if (rows != null && !rows.isEmpty()) {
            return rows.size();
        }
        if (rowMaps != null && !rowMaps.isEmpty()) {
            return rowMaps.size();
        }
        return 0;
    }

    private Boolean guessHasResultSet() {
        return !(columns == null || columns.isEmpty()) || (rows != null && !rows.isEmpty()) || (rowMaps != null && !rowMaps.isEmpty());
    }

    private static <T> List<T> defaultList(List<T> list) {
        if (list == null) {
            return List.of();
        }
        if (list instanceof ArrayList) {
            return list;
        }
        return List.copyOf(list);
    }

    public static Builder builder(String sql) {
        return new Builder(sql);
    }

    public static final class Builder {
        private String sql;
        private List<String> columns;
        private List<ColumnDef> columnDefs;
        private List<List<String>> rows;
        private List<Map<String, String>> rowMaps;
        private Integer rowsCount;
        private Boolean success;
        private String message;
        private String jobId;
        private String status;
        private Integer progressPercent;
        private Long elapsedMillis;
        private Long durationMillis;
        private Integer rowsAffected;
        private Integer returnedRowCount;
        private Integer actualRowCount;
        private Integer maxVisibleRows;
        private Integer maxTotalRows;
        private Integer totalRows;
        private Boolean hasResultSet;
        private Integer page;
        private Integer pageSize;
        private Boolean hasNext;
        private Boolean truncated;
        private String note;
        private Long queuedAt;
        private Long queueDelayMillis;
        private Boolean overloaded;
        private ThreadPoolSnapshot threadPool;
        private String commandTag;
        private Integer updateCount;
        private List<String> notices;
        private List<String> warnings;

        private Builder(String sql) {
            this.sql = sql;
        }

        public Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder columnDefs(List<ColumnDef> columnDefs) {
            this.columnDefs = columnDefs;
            return this;
        }

        public Builder rows(List<List<String>> rows) {
            this.rows = rows;
            return this;
        }

        public Builder rowMaps(List<Map<String, String>> rowMaps) {
            this.rowMaps = rowMaps;
            return this;
        }

        public Builder rowCount(Integer rowsCount) {
            this.rowsCount = rowsCount;
            return this;
        }

        public Builder success(Boolean success) {
            this.success = success;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder progressPercent(Integer progressPercent) {
            this.progressPercent = progressPercent;
            return this;
        }

        public Builder elapsedMillis(Long elapsedMillis) {
            this.elapsedMillis = elapsedMillis;
            return this;
        }

        public Builder durationMillis(Long durationMillis) {
            this.durationMillis = durationMillis;
            return this;
        }

        public Builder rowsAffected(Integer rowsAffected) {
            this.rowsAffected = rowsAffected;
            return this;
        }

        public Builder returnedRowCount(Integer returnedRowCount) {
            this.returnedRowCount = returnedRowCount;
            return this;
        }

        public Builder actualRowCount(Integer actualRowCount) {
            this.actualRowCount = actualRowCount;
            return this;
        }

        public Builder maxVisibleRows(Integer maxVisibleRows) {
            this.maxVisibleRows = maxVisibleRows;
            return this;
        }

        public Builder maxTotalRows(Integer maxTotalRows) {
            this.maxTotalRows = maxTotalRows;
            return this;
        }

        public Builder totalRows(Integer totalRows) {
            this.totalRows = totalRows;
            return this;
        }

        public Builder hasResultSet(Boolean hasResultSet) {
            this.hasResultSet = hasResultSet;
            return this;
        }

        public Builder page(Integer page) {
            this.page = page;
            return this;
        }

        public Builder pageSize(Integer pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder hasNext(Boolean hasNext) {
            this.hasNext = hasNext;
            return this;
        }

        public Builder truncated(Boolean truncated) {
            this.truncated = truncated;
            return this;
        }

        public Builder note(String note) {
            this.note = note;
            return this;
        }

        public Builder queuedAt(Long queuedAt) {
            this.queuedAt = queuedAt;
            return this;
        }

        public Builder queueDelayMillis(Long queueDelayMillis) {
            this.queueDelayMillis = queueDelayMillis;
            return this;
        }

        public Builder overloaded(Boolean overloaded) {
            this.overloaded = overloaded;
            return this;
        }

        public Builder threadPool(ThreadPoolSnapshot threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder commandTag(String commandTag) {
            this.commandTag = commandTag;
            return this;
        }

        public Builder updateCount(Integer updateCount) {
            this.updateCount = updateCount;
            return this;
        }

        public Builder notices(List<String> notices) {
            this.notices = notices;
            return this;
        }

        public Builder warnings(List<String> warnings) {
            this.warnings = warnings;
            return this;
        }

        public SqlExecResult build() {
            return new SqlExecResult(this);
        }
    }

    public String getSql() {
        return sql;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    public List<java.util.Map<String, String>> getRowMaps() {
        return rowMaps;
    }

    public int getRowsCount() {
        return rowsCount;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public String getJobId() {
        return jobId;
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

    public Long getDurationMillis() {
        return durationMillis;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
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

    public Integer getTotalRows() {
        return totalRows;
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

    public String getNote() {
        return note;
    }

    public Long getQueuedAt() {
        return queuedAt;
    }

    public Long getQueueDelayMillis() {
        return queueDelayMillis;
    }

    public Boolean getOverloaded() {
        return overloaded;
    }

    public ThreadPoolSnapshot getThreadPool() {
        return threadPool;
    }

    public String getCommandTag() {
        return commandTag;
    }

    public Integer getUpdateCount() {
        return updateCount;
    }

    public List<String> getNotices() {
        return notices;
    }

    public List<String> getWarnings() {
        return warnings;
    }
}
