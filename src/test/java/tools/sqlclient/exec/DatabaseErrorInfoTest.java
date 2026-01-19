package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseErrorInfoTest {

    @Test
    void shouldBuildFromPsqlException() {
        ServerErrorMessage serverErrorMessage = new ServerErrorMessage(buildServerErrorMessage());
        PSQLException psqlException = new PSQLException(serverErrorMessage);

        DatabaseErrorInfo info = DatabaseErrorInfo.fromSQLException(psqlException);
        assertNotNull(info);
        assertNotNull(info.getMessage());
        assertTrue(info.getMessage().contains("syntax error at or near \"asselect\""));
        assertEquals("42601", info.getSqlState());
        assertEquals(0, info.getVendorCode());
        assertEquals(12, info.getPosition());
        assertEquals("detail info", info.getDetail());
        assertEquals("hint info", info.getHint());
        assertEquals("where block", info.getWhere());
        assertEquals("public", info.getSchema());
        assertEquals("t1", info.getTable());
        assertEquals("c1", info.getColumn());
        assertEquals("text", info.getDatatype());
        assertEquals("pk_t1", info.getConstraint());
        assertEquals("parse.c", info.getFile());
        assertEquals("123", info.getLine());
        assertEquals("parser", info.getRoutine());
        String raw = info.getRaw();
        assertNotNull(raw);
        String[] lines = raw.split("\n");
        assertTrue(lines[0].contains("syntax error at or near \"asselect\""));
        assertTrue(containsLine(lines, "SQLSTATE: 42601"));
        assertTrue(containsLine(lines, "Position: 12"));
    }

    @Test
    void shouldUnwrapRootSQLException() {
        SQLException root = new SQLException("root", "22000", 12);
        RuntimeException wrapper = new RuntimeException("wrap", new RuntimeException(root));

        DatabaseErrorInfo info = DatabaseErrorInfo.fromSQLException(wrapper);
        assertNotNull(info);
        assertEquals("root", info.getMessage());
        assertEquals("22000", info.getSqlState());
        assertEquals(12, info.getVendorCode());
    }

    @Test
    void shouldChooseDisplayMessageInPriorityOrder() {
        DatabaseErrorInfo errorInfo = DatabaseErrorInfo.builder()
                .message("primary message")
                .sqlState("P1")
                .position(5)
                .detail("detail")
                .build();
        String display = ErrorDisplayFormatter.chooseDisplayMessage(errorInfo, "legacy error", "message", "FAILED");
        assertEquals(errorInfo.getRaw(), display);

        String fallback = ErrorDisplayFormatter.chooseDisplayMessage(null, "legacy error", "message", "FAILED");
        assertEquals("legacy error", fallback);

        String statusFallback = ErrorDisplayFormatter.chooseDisplayMessage(null, null, null, "FAILED");
        assertEquals("FAILED", statusFallback);

        String emptyFallback = ErrorDisplayFormatter.chooseDisplayMessage(null, "   ", null, null);
        assertNull(emptyFallback);
    }

    private String buildServerErrorMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append('S').append("ERROR").append('\0');
        sb.append('C').append("42601").append('\0');
        sb.append('M').append("syntax error at or near \"asselect\"").append('\0');
        sb.append('D').append("detail info").append('\0');
        sb.append('H').append("hint info").append('\0');
        sb.append('P').append("12").append('\0');
        sb.append('W').append("where block").append('\0');
        sb.append('s').append("public").append('\0');
        sb.append('t').append("t1").append('\0');
        sb.append('c').append("c1").append('\0');
        sb.append('d').append("text").append('\0');
        sb.append('n').append("pk_t1").append('\0');
        sb.append('F').append("parse.c").append('\0');
        sb.append('L').append("123").append('\0');
        sb.append('R').append("parser").append('\0');
        return sb.toString();
    }

    private boolean containsLine(String[] lines, String expected) {
        if (lines == null) {
            return false;
        }
        for (String line : lines) {
            if (expected.equals(line)) {
                return true;
            }
        }
        return false;
    }
}
