package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

class DatabaseErrorInfoTest {

    @Test
    void shouldBuildFromPsqlException() {
        ServerErrorMessage serverErrorMessage = Mockito.mock(ServerErrorMessage.class);
        when(serverErrorMessage.getDetail()).thenReturn("detail info");
        when(serverErrorMessage.getHint()).thenReturn("hint info");
        when(serverErrorMessage.getPosition()).thenReturn(12);
        when(serverErrorMessage.getWhere()).thenReturn("where block");
        when(serverErrorMessage.getSchema()).thenReturn("public");
        when(serverErrorMessage.getTable()).thenReturn("t1");
        when(serverErrorMessage.getColumn()).thenReturn("c1");
        when(serverErrorMessage.getDatatype()).thenReturn("text");
        when(serverErrorMessage.getConstraint()).thenReturn("pk_t1");
        when(serverErrorMessage.getFile()).thenReturn("parse.c");
        when(serverErrorMessage.getLine()).thenReturn("123");
        when(serverErrorMessage.getRoutine()).thenReturn("parser");

        PSQLException psqlException = Mockito.mock(PSQLException.class);
        when(psqlException.getMessage()).thenReturn("syntax error at or near \"asselect\"");
        when(psqlException.getSQLState()).thenReturn("42601");
        when(psqlException.getErrorCode()).thenReturn(0);
        when(psqlException.getServerErrorMessage()).thenReturn(serverErrorMessage);

        DatabaseErrorInfo info = DatabaseErrorInfo.fromSQLException(psqlException);
        assertNotNull(info);
        assertEquals("syntax error at or near \"asselect\"", info.getMessage());
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
        assertEquals("syntax error at or near \"asselect\"", lines[0]);
        assertEquals("SQLSTATE: 42601", lines[1]);
        assertEquals("Position: 12", lines[2]);
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
}
