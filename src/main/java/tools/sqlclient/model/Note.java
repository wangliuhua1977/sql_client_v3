package tools.sqlclient.model;

public class Note {
    private long id;
    private String title;
    private String content;
    private DatabaseType databaseType;
    private long createdAt;
    private long updatedAt;
    private String tags;
    private boolean starred;

    public Note(long id, String title, String content, DatabaseType databaseType, long createdAt, long updatedAt, String tags, boolean starred) {
        this.id = id;
        this.title = title;
        this.content = content;
        this.databaseType = databaseType;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.tags = tags == null ? "" : tags;
        this.starred = starred;
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

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags == null ? "" : tags;
    }

    public boolean isStarred() {
        return starred;
    }

    public void setStarred(boolean starred) {
        this.starred = starred;
    }

    @Override
    public String toString() {
        return "[" + databaseType + "] " + title;
    }
}
