package tools.sqlclient.importer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ImportReport {
    public enum Mode {CREATE, TRUNCATE_INSERT, APPEND}

    private String sourceType;
    private String sourceDetail;
    private String schema = "leshan";
    private String tableName;
    private Mode mode;
    private List<String> header = new ArrayList<>();
    private long sourceRows;
    private long insertedRows;
    private long skippedRows;
    private long failedRows;
    private long batchCount;
    private int maxSqlChars;
    private String errorMessage;
    private Instant startTime;
    private Instant endTime;
    private boolean cancelled;

    public String toText() {
        StringBuilder sb = new StringBuilder();
        sb.append("数据源: ").append(sourceType).append(" ").append(sourceDetail).append('\n');
        sb.append("目标: ").append(schema).append('.').append(tableName).append('\n');
        sb.append("模式: ").append(mode).append('\n');
        sb.append("列: ").append(String.join(", ", header)).append('\n');
        sb.append("开始: ").append(startTime).append(" 结束: ").append(endTime).append('\n');
        if (startTime != null && endTime != null) {
            sb.append("耗时: ").append(Duration.between(startTime, endTime).toMillis()).append(" ms\n");
        }
        sb.append("sourceRowCount=").append(sourceRows).append(" insertedRowCount=").append(insertedRows)
                .append(" skippedRows=").append(skippedRows).append(" failedRows=").append(failedRows).append('\n');
        sb.append("batchCount=").append(batchCount).append(" maxSqlChars=").append(maxSqlChars).append('\n');
        sb.append("cancelled=").append(cancelled).append('\n');
        if (errorMessage != null) {
            sb.append("错误: ").append(errorMessage).append('\n');
        }
        return sb.toString();
    }

    // getters and setters
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceDetail() {
        return sourceDetail;
    }

    public void setSourceDetail(String sourceDetail) {
        this.sourceDetail = sourceDetail;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public List<String> getHeader() {
        return header;
    }

    public void setHeader(List<String> header) {
        this.header = header;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public long getInsertedRows() {
        return insertedRows;
    }

    public void setInsertedRows(long insertedRows) {
        this.insertedRows = insertedRows;
    }

    public long getSkippedRows() {
        return skippedRows;
    }

    public void setSkippedRows(long skippedRows) {
        this.skippedRows = skippedRows;
    }

    public long getFailedRows() {
        return failedRows;
    }

    public void setFailedRows(long failedRows) {
        this.failedRows = failedRows;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(long batchCount) {
        this.batchCount = batchCount;
    }

    public int getMaxSqlChars() {
        return maxSqlChars;
    }

    public void setMaxSqlChars(int maxSqlChars) {
        this.maxSqlChars = maxSqlChars;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
