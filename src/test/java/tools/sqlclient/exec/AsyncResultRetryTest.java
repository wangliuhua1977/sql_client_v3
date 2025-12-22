package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.sqlclient.exec.AsyncResultConfig.PageBase;
import tools.sqlclient.exec.AsyncResultConfig.RemoveAfterFetchStrategy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncResultRetryTest {
    private final Gson gson = new Gson();
    private final List<String> propsToClear = new ArrayList<>();

    @BeforeEach
    void setupDefaults() {
        overrideProp("ASYNC_SQL_RESULT_RETRY_BASE_DELAY_MS", "10");
        overrideProp("ASYNC_SQL_RESULT_RETRY_MAX_DELAY_MS", "20");
        overrideProp("ASYNC_SQL_RESULT_RETRY_MAX", "4");
        overrideProp("ASYNC_SQL_RESULT_FETCH_REMOVE_AFTER", RemoveAfterFetchStrategy.FALSE.name());
        overrideProp("ASYNC_SQL_RESULT_PAGE_BASE", PageBase.AUTO.name());
    }

    @AfterEach
    void cleanupProps() {
        for (String key : propsToClear) {
            System.clearProperty(key);
        }
    }

    @Test
    void shouldRecoverFromTransientExpiry() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(jsonResponse(statusPayload("job-1", "SUBMITTED", null, false)));
            server.enqueue(jsonResponse(statusPayload("job-1", "SUCCEEDED", 1000, true)));
            server.enqueue(jsonResponse(resultExpiredPayload(1, 200)));
            server.enqueue(jsonResponse(resultSuccessPayload(1, 1000, false)));
            server.start();

            overrideProp("ASYNC_SQL_BASE_URL", server.url("/").toString());
            SqlExecutionService service = new SqlExecutionService();
            SqlExecResult result = service.executeSyncAllPagesWithDbUser("select * from information_schema.tables", "tester", 200);

            assertEquals(1000, result.getRows().size(), "应在自愈后成功解析全部元数据行");
            assertEquals("SUCCEEDED", result.getStatus());

            // /jobs/result 应被调用两次（首次过期 + 重试成功）
            assertEquals(4, server.getRequestCount());
            RecordedRequest firstResultReq = server.takeRequest(); // submit
            RecordedRequest statusReq = server.takeRequest(); // status
            RecordedRequest expiredReq = server.takeRequest(); // first result
            RecordedRequest successReq = server.takeRequest(); // retry result
            assertTrue(expiredReq.getBody().readUtf8().contains("\"page\":1"));
            assertTrue(successReq.getBody().readUtf8().contains("\"page\":1"));
        }
    }

    @Test
    void shouldFallbackPageBaseWhenNeeded() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            AtomicInteger resultCalls = new AtomicInteger(0);
            server.setDispatcher(new Dispatcher() {
                @Override
                public MockResponse dispatch(RecordedRequest request) {
                    String path = request.getPath();
                    if (path != null && path.endsWith("/jobs/submit")) {
                        return jsonResponse(statusPayload("job-2", "SUBMITTED", null, false));
                    }
                    if (path != null && path.endsWith("/jobs/status")) {
                        return jsonResponse(statusPayload("job-2", "SUCCEEDED", 3, true));
                    }
                    if (path != null && path.endsWith("/jobs/result")) {
                        int call = resultCalls.incrementAndGet();
                        String body = request.getBody().readString(StandardCharsets.UTF_8);
                        if (call == 1) {
                            assertTrue(body.contains("\"page\":1"), "首次尝试应使用现有 page 基准");
                            return jsonResponse(resultExpiredPayload(1, 50));
                        }
                        assertTrue(body.contains("\"page\":0"), "回退后应尝试另一种 page 基准");
                        return jsonResponse(resultSuccessPayload(0, 3, true));
                    }
                    return new MockResponse().setResponseCode(404);
                }
            });
            server.start();

            overrideProp("ASYNC_SQL_BASE_URL", server.url("/").toString());
            SqlExecutionService service = new SqlExecutionService();
            SqlExecResult result = service.executeSyncAllPagesWithDbUser("select * from information_schema.views", "tester", 50);

            assertEquals(3, result.getRows().size(), "应在 page 基准回退后成功解析结果");
            assertEquals("SUCCEEDED", result.getStatus());
        }
    }

    private void overrideProp(String key, String value) {
        System.setProperty(key, value);
        propsToClear.add(key);
    }

    private MockResponse jsonResponse(JsonObject payload) {
        return new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody(gson.toJson(payload));
    }

    private JsonObject statusPayload(String jobId, String status, Integer actualRowCount, boolean hasResultSet) {
        JsonObject obj = new JsonObject();
        obj.addProperty("jobId", jobId);
        obj.addProperty("status", status);
        obj.addProperty("success", "SUCCEEDED".equalsIgnoreCase(status));
        obj.addProperty("hasResultSet", hasResultSet);
        if (actualRowCount != null) {
            obj.addProperty("actualRowCount", actualRowCount);
        }
        obj.addProperty("returnedRowCount", actualRowCount == null ? 0 : actualRowCount);
        obj.addProperty("progressPercent", 100);
        return obj;
    }

    private JsonObject resultExpiredPayload(int page, int pageSize) {
        JsonObject obj = new JsonObject();
        obj.addProperty("success", false);
        obj.addProperty("status", "SUCCEEDED");
        obj.addProperty("message", "Result expired or not available anymore");
        obj.addProperty("page", page);
        obj.addProperty("pageSize", pageSize);
        obj.addProperty("hasResultSet", true);
        obj.addProperty("actualRowCount", 1000);
        obj.addProperty("returnedRowCount", 0);
        obj.add("resultRows", gson.toJsonTree(List.of()));
        return obj;
    }

    private JsonObject resultSuccessPayload(int page, int count, boolean zeroBased) {
        JsonObject obj = new JsonObject();
        obj.addProperty("success", true);
        obj.addProperty("status", "SUCCEEDED");
        obj.addProperty("page", page);
        obj.addProperty("pageSize", count);
        obj.addProperty("hasResultSet", true);
        obj.addProperty("returnedRowCount", count);
        obj.addProperty("actualRowCount", count);
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(List.of((zeroBased ? "view_" : "table_") + i));
        }
        obj.add("resultRows", gson.toJsonTree(rows));
        obj.add("columns", gson.toJsonTree(List.of("object_name")));
        return obj;
    }
}
