package tools.sqlclient.importer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class ImportReport {
    private String sourceType;
    private String sourceDescription;
    private String targetTable;
    private List<ColumnSpec> columns;
    private long sourceRowCount;
    private long insertedRows;
    private long skippedRows;
    private long failedRows;
    private int batchCount;
    private Instant start;
    private Instant end;
    private String errorMessage;

    public String format() {
        StringBuilder sb = new StringBuilder();
        sb.append("数据源: ").append(sourceType).append("\n");
        sb.append("来源描述: ").append(sourceDescription).append("\n");
        sb.append("目标表: ").append(targetTable).append("\n");
        sb.append("列: \n");
        if (columns != null) {
            for (ColumnSpec c : columns) {
                sb.append("  ").append(c.getOriginalHeader()).append(" -> ").append(c.getNormalizedName())
                        .append(" (").append(c.getInferredType()).append(")\n");
            }
        }
        sb.append("总行数: ").append(sourceRowCount).append("\n");
        sb.append("成功插入: ").append(insertedRows).append("\n");
        sb.append("去重跳过: ").append(skippedRows).append("\n");
        sb.append("失败: ").append(failedRows).append("\n");
        sb.append("批次数: ").append(batchCount).append("\n");
        if (start != null && end != null) {
            sb.append("耗时: ").append(Duration.between(start, end).toMillis()).append(" ms\n");
        }
        if (errorMessage != null) {
            sb.append("错误: ").append(errorMessage).append("\n");
        }
        return sb.toString();
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceDescription() {
        return sourceDescription;
    }

    public void setSourceDescription(String sourceDescription) {
        this.sourceDescription = sourceDescription;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public List<ColumnSpec> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnSpec> columns) {
        this.columns = columns;
    }

    public long getSourceRowCount() {
        return sourceRowCount;
    }

    public void setSourceRowCount(long sourceRowCount) {
        this.sourceRowCount = sourceRowCount;
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

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public Instant getStart() {
        return start;
    }

    public void setStart(Instant start) {
        this.start = start;
    }

    public Instant getEnd() {
        return end;
    }

    public void setEnd(Instant end) {
        this.end = end;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
