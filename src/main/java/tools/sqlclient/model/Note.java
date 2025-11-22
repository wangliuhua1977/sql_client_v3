package tools.sqlclient.model;

public class Note {
    private long id;
    private String title;
    private String content;
    private DatabaseType databaseType;
    private long updatedAt;

    public Note(long id, String title, String content, DatabaseType databaseType, long updatedAt) {
        this.id = id;
        this.title = title;
        this.content = content;
        this.databaseType = databaseType;
        this.updatedAt = updatedAt;
    }

    public long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(DatabaseType databaseType) {
        this.databaseType = databaseType;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return "[" + databaseType + "] " + title;
    }
}
