package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StatusErrorResultBuilderTest {
    private final Gson gson = new Gson();

    @Test
    void shouldParsePositionFromErrorMessage() {
        String json = "{" +
                "\"status\":\"FAILED\"," +
                "\"errorMessage\":\"ERROR: relation \\\"t1\\\" does not exist\\n  Position: 30\"," +
                "\"sqlSummary\":\"select * from t1\"," +
                "\"jobId\":\"4490\"}";
        JsonObject obj = gson.fromJson(json, JsonObject.class);

        TableErrorFormatter.ErrorTable table = StatusErrorResultBuilder.buildErrorResultTable(obj);

        assertNotNull(table);
        assertEquals(2, table.columnDefs().size());
        assertEquals("ERROR_MESSAGE", table.columnDefs().getFirst().getDisplayName());
        assertEquals(1, table.rows().size());
        assertTrue(table.rows().getFirst().getFirst().contains("relation \"t1\" does not exist"));
        assertEquals("30", table.rows().getFirst().get(1));
    }

    @Test
    void shouldPreferNumericPositionField() {
        String json = "{" +
                "\"status\":\"FAILED\"," +
                "\"errorMessage\":\"ERROR: ... Position: 99\"," +
                "\"position\":12}";
        JsonObject obj = gson.fromJson(json, JsonObject.class);

        TableErrorFormatter.ErrorTable table = StatusErrorResultBuilder.buildErrorResultTable(obj);

        assertEquals("12", table.rows().getFirst().get(1));
    }

    @Test
    void shouldAllowMissingPosition() {
        String json = "{" +
                "\"status\":\"FAILED\"," +
                "\"errorMessage\":\"ERROR: something bad\"}";
        JsonObject obj = gson.fromJson(json, JsonObject.class);

        TableErrorFormatter.ErrorTable table = StatusErrorResultBuilder.buildErrorResultTable(obj);

        assertEquals(1, table.rows().size());
        assertEquals("", table.rows().getFirst().get(1));
    }

    @Test
    void shouldFallbackWhenErrorMessageMissing() {
        String json = "{" +
                "\"status\":\"FAILED\"," +
                "\"jobId\":\"x\"}";
        JsonObject obj = gson.fromJson(json, JsonObject.class);

        TableErrorFormatter.ErrorTable table = StatusErrorResultBuilder.buildErrorResultTable(obj);

        assertEquals("数据库未返回错误信息", table.rows().getFirst().getFirst());
        assertFalse(table.rows().getFirst().getFirst().contains("任务x 状态 FAILED"));
    }
}

