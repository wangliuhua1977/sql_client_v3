package tools.sqlclient.importer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ImportReport {
    private String sourceType;
    private String sourcePath;
    private String tableName;
    private boolean cancelled;
    private long sourceRowCount;
    private long insertedRowCount;
    private long dedupSkippedRowCount;
    private long failedRowCount;
    private int batchCount;
    private Instant startTime = Instant.now();
    private Instant endTime;
    private final List<String> warnings = new ArrayList<>();

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public long getSourceRowCount() {
        return sourceRowCount;
    }

    public void setSourceRowCount(long sourceRowCount) {
        this.sourceRowCount = sourceRowCount;
    }

    public long getInsertedRowCount() {
        return insertedRowCount;
    }

    public void setInsertedRowCount(long insertedRowCount) {
        this.insertedRowCount = insertedRowCount;
    }

    public long getDedupSkippedRowCount() {
        return dedupSkippedRowCount;
    }

    public void setDedupSkippedRowCount(long dedupSkippedRowCount) {
        this.dedupSkippedRowCount = dedupSkippedRowCount;
    }

    public long getFailedRowCount() {
        return failedRowCount;
    }

    public void setFailedRowCount(long failedRowCount) {
        this.failedRowCount = failedRowCount;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void incrementBatchCount() {
        this.batchCount++;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void markStart() {
        this.startTime = Instant.now();
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void markEnd() {
        this.endTime = Instant.now();
    }

    public void addWarning(String warning) {
        this.warnings.add(warning);
    }

    public String toText() {
        String duration = endTime != null ? Duration.between(startTime, endTime).toMillis() + " ms" : "";
        return "表格导入报告" +
                "\n源类型: " + sourceType +
                "\n源路径: " + sourcePath +
                "\n目标表: leshan." + tableName +
                "\n行数: 源=" + sourceRowCount + " 插入=" + insertedRowCount + " 跳过=" + dedupSkippedRowCount + " 失败=" + failedRowCount +
                "\n批次数: " + batchCount +
                "\n耗时: " + duration +
                (cancelled ? "\n状态: 已取消" : "\n状态: 完成") +
                (warnings.isEmpty() ? "" : "\n警告:\n" + warnings.stream().collect(Collectors.joining("\n")));
    }
}
