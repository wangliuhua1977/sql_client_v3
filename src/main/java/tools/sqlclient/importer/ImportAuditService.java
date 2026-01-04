package tools.sqlclient.importer;

/**
 * 简易稽核：校验行数关系。
 */
public class ImportAuditService {
    public boolean auditCounts(ImportExecutionService.Mode mode, ImportReport report) {
        if (mode == ImportExecutionService.Mode.APPEND_DEDUP) {
            return report.getInsertedRows() + report.getSkippedRows() == report.getSourceRowCount();
        }
        return report.getInsertedRows() == report.getSourceRowCount();
    }
}
