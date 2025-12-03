package tools.sqlclient.model;

import java.util.Objects;

/**
 * SQL 片段实体，包含名称、标签、正文以及时间戳。
 */
public class SqlSnippet {
    private long id;
    private String name;
    private String tags;
    private String content;
    private long createdAt;
    private long updatedAt;

    public SqlSnippet(long id, String name, String tags, String content, long createdAt, long updatedAt) {
        this.id = id;
        this.name = name;
        this.tags = tags;
        this.content = content;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public SqlSnippet(String name, String tags, String content) {
        this(0, name, tags, content, System.currentTimeMillis(), System.currentTimeMillis());
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlSnippet that = (SqlSnippet) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
