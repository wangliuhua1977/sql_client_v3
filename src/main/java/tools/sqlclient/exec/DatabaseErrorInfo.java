package tools.sqlclient.exec;

import com.google.gson.JsonObject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 统一的数据库错误模型，优先透传 PostgreSQL JDBC 的原始字段。
 */
public class DatabaseErrorInfo {
    private final String message;
    private final String sqlState;
    private final Integer vendorCode;
    private final String detail;
    private final String hint;
    private final Integer position;
    private final String where;
    private final String schema;
    private final String table;
    private final String column;
    private final String datatype;
    private final String constraint;
    private final String file;
    private final String line;
    private final String routine;
    private final String raw;

    private DatabaseErrorInfo(Builder builder) {
        this.message = builder.message;
        this.sqlState = builder.sqlState;
        this.vendorCode = builder.vendorCode;
        this.detail = builder.detail;
        this.hint = builder.hint;
        this.position = builder.position;
        this.where = builder.where;
        this.schema = builder.schema;
        this.table = builder.table;
        this.column = builder.column;
        this.datatype = builder.datatype;
        this.constraint = builder.constraint;
        this.file = builder.file;
        this.line = builder.line;
        this.routine = builder.routine;
        this.raw = builder.raw != null ? builder.raw : buildRaw();
    }

    public String getMessage() {
        return message;
    }

    public String getSqlState() {
        return sqlState;
    }

    public Integer getVendorCode() {
        return vendorCode;
    }

    public String getDetail() {
        return detail;
    }

    public String getHint() {
        return hint;
    }

    public Integer getPosition() {
        return position;
    }

    public String getWhere() {
        return where;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public String getDatatype() {
        return datatype;
    }

    public String getConstraint() {
        return constraint;
    }

    public String getFile() {
        return file;
    }

    public String getLine() {
        return line;
    }

    public String getRoutine() {
        return routine;
    }

    public String getRaw() {
        return raw;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DatabaseErrorInfo fromJson(JsonObject obj) {
        if (obj == null) {
            return null;
        }
        Builder builder = builder();
        builder.message = readString(obj, "message");
        builder.sqlState = readString(obj, "sqlState");
        builder.vendorCode = readInteger(obj, "vendorCode");
        builder.detail = readString(obj, "detail");
        builder.hint = readString(obj, "hint");
        builder.position = readInteger(obj, "position");
        builder.where = readString(obj, "where");
        builder.schema = readString(obj, "schema");
        builder.table = readString(obj, "table");
        builder.column = readString(obj, "column");
        builder.datatype = readString(obj, "datatype");
        builder.constraint = readString(obj, "constraint");
        builder.file = readString(obj, "file");
        builder.line = readString(obj, "line");
        builder.routine = readString(obj, "routine");
        builder.raw = readString(obj, "raw");
        return builder.build();
    }

    public static DatabaseErrorInfo fromSQLException(Throwable throwable) {
        SQLException sqlEx = unwrapSQLException(throwable);
        if (sqlEx == null) {
            return null;
        }
        Builder builder = builder();
        builder.message = sqlEx.getMessage();
        builder.sqlState = sqlEx.getSQLState();
        builder.vendorCode = sqlEx.getErrorCode();
        if (sqlEx instanceof PSQLException psqlException) {
            ServerErrorMessage serverError = psqlException.getServerErrorMessage();
            if (serverError != null) {
                builder.detail = serverError.getDetail();
                builder.hint = serverError.getHint();
                builder.position = serverError.getPosition();
                builder.where = serverError.getWhere();
                builder.schema = serverError.getSchema();
                builder.table = serverError.getTable();
                builder.column = serverError.getColumn();
                builder.datatype = serverError.getDatatype();
                builder.constraint = serverError.getConstraint();
                builder.file = serverError.getFile();
                builder.line = serverError.getLine();
                builder.routine = serverError.getRoutine();
            }
        }
        return builder.build();
    }

    private static SQLException unwrapSQLException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException se) {
                return se;
            }
            current = current.getCause();
        }
        return null;
    }

    private String buildRaw() {
        List<String> lines = new ArrayList<>();
        if (message != null && !message.isBlank()) {
            lines.add(message);
        }
        if (sqlState != null && !sqlState.isBlank()) {
            lines.add("SQLSTATE: " + sqlState);
        }
        if (position != null) {
            lines.add("Position: " + position);
        }
        if (detail != null && !detail.isBlank()) {
            lines.add("Detail: " + detail);
        }
        if (hint != null && !hint.isBlank()) {
            lines.add("Hint: " + hint);
        }
        if (where != null && !where.isBlank()) {
            lines.add("Where: " + where);
        }
        return String.join("\n", lines);
    }

    private static String readString(JsonObject obj, String key) {
        if (!obj.has(key) || obj.get(key).isJsonNull()) {
            return null;
        }
        return obj.get(key).getAsString();
    }

    private static Integer readInteger(JsonObject obj, String key) {
        if (!obj.has(key) || obj.get(key).isJsonNull()) {
            return null;
        }
        try {
            return obj.get(key).getAsInt();
        } catch (Exception e) {
            return null;
        }
    }

    public static final class Builder {
        private String message;
        private String sqlState;
        private Integer vendorCode;
        private String detail;
        private String hint;
        private Integer position;
        private String where;
        private String schema;
        private String table;
        private String column;
        private String datatype;
        private String constraint;
        private String file;
        private String line;
        private String routine;
        private String raw;

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder sqlState(String sqlState) {
            this.sqlState = sqlState;
            return this;
        }

        public Builder vendorCode(Integer vendorCode) {
            this.vendorCode = vendorCode;
            return this;
        }

        public Builder detail(String detail) {
            this.detail = detail;
            return this;
        }

        public Builder hint(String hint) {
            this.hint = hint;
            return this;
        }

        public Builder position(Integer position) {
            this.position = position;
            return this;
        }

        public Builder where(String where) {
            this.where = where;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder column(String column) {
            this.column = column;
            return this;
        }

        public Builder datatype(String datatype) {
            this.datatype = datatype;
            return this;
        }

        public Builder constraint(String constraint) {
            this.constraint = constraint;
            return this;
        }

        public Builder file(String file) {
            this.file = file;
            return this;
        }

        public Builder line(String line) {
            this.line = line;
            return this;
        }

        public Builder routine(String routine) {
            this.routine = routine;
            return this;
        }

        public Builder raw(String raw) {
            this.raw = raw;
            return this;
        }

        public DatabaseErrorInfo build() {
            if (message == null) {
                message = "未知数据库错误";
            }
            return new DatabaseErrorInfo(this);
        }
    }
}
