package tools.sqlclient.importer;

import java.util.Objects;

/**
 * 描述字段名与推断类型。
 */
public class ColumnSpec {
    private final String originalHeader;
    private final String normalizedName;
    private final String inferredType;

    public ColumnSpec(String originalHeader, String normalizedName, String inferredType) {
        this.originalHeader = originalHeader;
        this.normalizedName = normalizedName;
        this.inferredType = inferredType;
    }

    public String getOriginalHeader() {
        return originalHeader;
    }

    public String getNormalizedName() {
        return normalizedName;
    }

    public String getInferredType() {
        return inferredType;
    }

    public ColumnSpec withType(String type) {
        return new ColumnSpec(originalHeader, normalizedName, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnSpec that)) return false;
        return Objects.equals(originalHeader, that.originalHeader)
                && Objects.equals(normalizedName, that.normalizedName)
                && Objects.equals(inferredType, that.inferredType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalHeader, normalizedName, inferredType);
    }
}
