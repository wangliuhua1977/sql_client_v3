package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TableErrorFormatterTest {

    @Test
    void shouldBuildErrorTableWithMessageAndPosition() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setJobId("x");
        resp.setErrorMessage("syntax error at or near \"asselect\"");
        resp.setPosition(123);

        TableErrorFormatter.ErrorTable table = TableErrorFormatter.buildErrorTable(resp);
        assertNotNull(table);
        assertEquals("syntax error at or near \"asselect\"\nPosition: 123", table.displayMessage());
        assertEquals(List.of("ERROR_MESSAGE", "POSITION"),
                table.columnDefs().stream().map(ColumnDef::getDisplayName).toList());
        assertEquals("syntax error at or near \"asselect\"", table.rows().get(0).get(0));
        assertEquals("123", table.rows().get(0).get(1));
    }

    @Test
    void shouldBuildErrorTableWithEmptyPosition() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setErrorMessage("bad sql");

        TableErrorFormatter.ErrorTable table = TableErrorFormatter.buildErrorTable(resp);
        assertNotNull(table);
        assertEquals("bad sql", table.displayMessage());
        assertEquals("", table.rows().get(0).get(1));
    }

    @Test
    void shouldReadCapitalizedPosition() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setPositionUpper(77);
        resp.setErrorMessage("syntax error");

        TableErrorFormatter.ErrorTable table = TableErrorFormatter.buildErrorTable(resp);
        assertEquals("77", table.rows().get(0).get(1));
        assertEquals("syntax error\nPosition: 77", table.displayMessage());
    }

    @Test
    void shouldFallbackToDefaultMessageWhenNoErrorFields() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setJobId("8dd01734-1429-42cb-bfca-7b5dc5f626e9");

        TableErrorFormatter.ErrorTable table = TableErrorFormatter.buildErrorTable(resp);
        assertNotNull(table);
        assertEquals("数据库未返回错误信息", table.rows().get(0).get(0));
        assertEquals("", table.rows().get(0).get(1));
        assertEquals("数据库未返回错误信息", table.displayMessage());
    }

    @Test
    void shouldUseErrorRawWhenMessageMissing() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        DatabaseErrorInfo error = DatabaseErrorInfo.builder()
                .raw("syntax error near FROM")
                .position(12)
                .build();
        resp.setError(error);

        TableErrorFormatter.ErrorTable table = TableErrorFormatter.buildErrorTable(resp);

        assertEquals("syntax error near FROM\nPosition: 12", table.displayMessage());
        assertEquals("syntax error near FROM", table.rows().get(0).get(0));
        assertEquals("12", table.rows().get(0).get(1));
    }
}

