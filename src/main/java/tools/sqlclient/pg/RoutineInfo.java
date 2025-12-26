package tools.sqlclient.pg;

/**
 * PostgreSQL routine 元信息，覆盖函数/存储过程。
 */
public record RoutineInfo(long oid,
                          String schemaName,
                          String objectName,
                          String kind,
                          String identityArgs,
                          String fullArgs,
                          String resultType) {

    public boolean isFunction() {
        return "f".equalsIgnoreCase(kind);
    }

    public boolean isProcedure() {
        return "p".equalsIgnoreCase(kind);
    }

    public String displayName() {
        String args = identityArgs == null ? "" : identityArgs;
        return (schemaName == null ? "" : schemaName + ".") + objectName + "(" + args + ")";
    }
}
