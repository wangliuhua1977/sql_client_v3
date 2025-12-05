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
    private boolean trashed;
    private long deletedAt;
    private String styleName;

    public Note(long id, String title, String content, DatabaseType databaseType,
                long createdAt, long updatedAt, String tags, boolean starred,
                boolean trashed, long deletedAt, String styleName) {
        this.id = id;
        this.title = title;
        this.content = content;
        this.databaseType = databaseType;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.tags = tags == null ? "" : tags;
        this.starred = starred;
        this.trashed = trashed;
        this.deletedAt = deletedAt;
        this.styleName = styleName == null ? "" : styleName;
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

    public boolean isTrashed() {
        return trashed;
    }

    public void setTrashed(boolean trashed) {
        this.trashed = trashed;
    }

    public long getDeletedAt() {
        return deletedAt;
    }

    public void setDeletedAt(long deletedAt) {
        this.deletedAt = deletedAt;
    }

    public String getStyleName() {
        return styleName;
    }

    public void setStyleName(String styleName) {
        this.styleName = styleName == null ? "" : styleName;
    }

    @Override
    public String toString() {
        return "[" + databaseType + "] " + title;
    }
}
