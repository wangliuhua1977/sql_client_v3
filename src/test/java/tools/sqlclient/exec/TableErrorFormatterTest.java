package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableErrorFormatterTest {

    @Test
    void shouldRenderErrorMessageAndPosition() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setJobId("x");
        resp.setErrorMessage("syntax error at or near \"asselect\"");
        resp.setPosition(123);

        String text = TableErrorFormatter.buildTableErrorText(resp);

        assertEquals("syntax error at or near \"asselect\"\nPosition: 123", text);
    }

    @Test
    void shouldRenderErrorMessageOnlyWhenNoPosition() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setErrorMessage("bad sql");

        String text = TableErrorFormatter.buildTableErrorText(resp);

        assertEquals("bad sql", text);
    }

    @Test
    void shouldRenderPositionOnly() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setPosition(77);

        String text = TableErrorFormatter.buildTableErrorText(resp);

        assertEquals("Position: 77", text);
    }

    @Test
    void shouldFallbackToJobStatusWhenNoErrorFields() {
        ResultResponse resp = new ResultResponse();
        resp.setStatus("FAILED");
        resp.setJobId("8dd01734-1429-42cb-bfca-7b5dc5f626e9");

        String text = TableErrorFormatter.buildTableErrorText(resp);

        assertEquals("任务8dd01734-1429-42cb-bfca-7b5dc5f626e9 状态 FAILED", text);
    }
}

