package tools.sqlclient.exec;

/**
 * 封装失败 SQL 的结构化错误，便于 UI 展示。
 */
public class SqlExecutionException extends RuntimeException {
    private final DatabaseErrorInfo errorInfo;
    private final Integer statementIndex;
    private final String sqlFragment;

    public SqlExecutionException(String message, DatabaseErrorInfo errorInfo) {
        this(message, errorInfo, null, null);
    }

    public SqlExecutionException(String message, DatabaseErrorInfo errorInfo, Integer statementIndex, String sqlFragment) {
        super(message);
        this.errorInfo = errorInfo;
        this.statementIndex = statementIndex;
        this.sqlFragment = sqlFragment;
    }

    public DatabaseErrorInfo getErrorInfo() {
        return errorInfo;
    }

    public Integer getStatementIndex() {
        return statementIndex;
    }

    public String getSqlFragment() {
        return sqlFragment;
    }
}
