package tools.sqlclient.model;

public class SqlHistoryEntry {
    private final long id;
    private final String sqlText;
    private final DatabaseType databaseType;
    private final long createdAt;
    private final long lastExecutedAt;

    public SqlHistoryEntry(long id, String sqlText, DatabaseType databaseType, long createdAt, long lastExecutedAt) {
        this.id = id;
        this.sqlText = sqlText == null ? "" : sqlText;
        this.databaseType = databaseType;
        this.createdAt = createdAt;
        this.lastExecutedAt = lastExecutedAt;
    }

    public long getId() {
        return id;
    }

    public String getSqlText() {
        return sqlText;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getLastExecutedAt() {
        return lastExecutedAt;
    }
}
