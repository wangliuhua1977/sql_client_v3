package tools.sqlclient.importer;

public class ColumnSpec {
    private String originalName;
    private String normalizedName;
    private String pgType;

    public ColumnSpec(String originalName, String normalizedName, String pgType) {
        this.originalName = originalName;
        this.normalizedName = normalizedName;
        this.pgType = pgType;
    }

    public String getOriginalName() {
        return originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public String getNormalizedName() {
        return normalizedName;
    }

    public void setNormalizedName(String normalizedName) {
        this.normalizedName = normalizedName;
    }

    public String getPgType() {
        return pgType;
    }

    public void setPgType(String pgType) {
        this.pgType = pgType;
    }
}
