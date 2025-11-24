package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.network.TrustAllHttpClient;
import tools.sqlclient.util.ThreadPools;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * SQL 远程执行服务：调用 HTTPS 接口获取结果集。
 */
public class SqlExecutionService {
    private static final String EXEC_API = "https://leshan.paas.sc.ctc.com/waf-dev/api?api_id=sql_exec&api_token=xxfs&sql_memo=";
    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Gson gson = new Gson();

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError) {
        String trimmed = sql.trim();
        if (trimmed.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        String encoded = URLEncoder.encode(trimmed, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(URI.create(EXEC_API + encoded)).GET().build();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApplyAsync(resp -> parseResponse(trimmed, resp.body()), ThreadPools.NETWORK_POOL)
                .thenAcceptAsync(onSuccess, ThreadPools.NETWORK_POOL)
                .exceptionally(ex -> {
                    if (onError != null) {
                        if (ex instanceof java.util.concurrent.CancellationException) {
                            onError.accept(new RuntimeException("执行已取消"));
                        } else {
                            onError.accept(new RuntimeException(ex));
                        }
                    }
                    return null;
                });
    }

    private SqlExecResult parseResponse(String sql, String body) {
        JsonObject obj = gson.fromJson(body, JsonObject.class);
        int rowsCount = obj.has("rows_count") && !obj.get("rows_count").isJsonNull()
                ? Integer.parseInt(obj.get("rows_count").getAsString()) : 0;
        JsonArray data = extractDataArray(obj);
        List<String> columns = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        if (data != null) {
            for (JsonElement el : data) {
                if (!el.isJsonObject()) continue;
                JsonObject rowObj = el.getAsJsonObject();
                if (columns.isEmpty()) {
                    for (Map.Entry<String, JsonElement> entry : rowObj.entrySet()) {
                        if (shouldSkip(entry.getKey())) continue;
                        columns.add(entry.getKey());
                    }
                }
                List<String> row = new ArrayList<>();
                for (String col : columns) {
                    JsonElement value = rowObj.get(col);
                    row.add(value == null || value.isJsonNull() ? "" : value.getAsString());
                }
                rows.add(row);
            }
        }
        return new SqlExecResult(sql, columns, rows, rowsCount);
    }

    private JsonArray extractDataArray(JsonObject obj) {
        if (obj == null) return new JsonArray();
        if (obj.has("Data") && obj.get("Data").isJsonArray()) {
            return obj.getAsJsonArray("Data");
        }
        for (String key : List.of("data", "rows", "list", "items", "result")) {
            if (obj.has(key) && obj.get(key).isJsonArray()) {
                return obj.getAsJsonArray(key);
            }
        }
        return new JsonArray();
    }

    private boolean shouldSkip(String key) {
        return "sn_".equalsIgnoreCase(key);
    }
}
