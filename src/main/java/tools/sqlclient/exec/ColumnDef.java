package tools.sqlclient.exec;

/**
 * 列定义：区分内部唯一标识与显示名，并记录取值来源 key。
 */
public class ColumnDef {
    private final String id;
    private final String displayName;
    private final String sourceKey;

    public ColumnDef(String id, String displayName, String sourceKey) {
        this.id = id;
        this.displayName = displayName;
        this.sourceKey = sourceKey;
    }

    public String getId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getSourceKey() {
        return sourceKey;
    }
}
