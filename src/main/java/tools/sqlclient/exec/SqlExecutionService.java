package tools.sqlclient.exec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import tools.sqlclient.network.TrustAllHttpClient;
import tools.sqlclient.util.OperationLog;
import tools.sqlclient.util.ThreadPools;

import java.net.URI;
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
    // 一定要用你真实的域名，不要写成带 ... 的鬼畜版本
    private static final String EXEC_API =
            "https://leshan.paas.sc.ctc.com/waf-dev/api?api_id=sql_exec&api_token=xxfs&sql_memo=";

    private final HttpClient httpClient = TrustAllHttpClient.create();
    private final Gson gson = new Gson();

    public CompletableFuture<Void> execute(String sql,
                                           Consumer<SqlExecResult> onSuccess,
                                           Consumer<Exception> onError) {
        String trimmed = (sql == null) ? "" : sql.trim();
        if (trimmed.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // 先输出本次将要执行的完整 SQL（可能是一整块，多行）
        OperationLog.log("即将执行 SQL（本次 Ctrl+Enter 文本，未拆分）:\n" + trimmed);

        // 关键修复：把所有换行符压成空格，避免 URI 中出现非法字符
        String normalized = trimmed
                .replace("\r\n", " ")
                .replace("\n", " ")
                .replace("\r", " ");

        // 按原有约定，用 $$ 包裹，不做额外转码
        String wrapped = "$$" + normalized + "$$";

        URI uri;
        try {
            // 先尝试直接拼接
            uri = URI.create(EXEC_API + wrapped);
        } catch (IllegalArgumentException ex) {
            // 极端兜底：仅把空格转成 %20，其它字符保持原状，依然不碰引号之类
            String fallback = wrapped.replace(" ", "%20");
            uri = URI.create(EXEC_API + fallback);
        }

        HttpRequest request = HttpRequest.newBuilder(uri).GET().build();

        // 输出请求 URL
        OperationLog.log("SQL 执行请求: " + uri);

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApplyAsync(resp -> {
                    OperationLog.log("SQL 执行响应: " + OperationLog.abbreviate(resp.body(), 400));
                    // 这里仍然用原始 trimmed 作为“本条 SQL 文本”，方便结果面板展示
                    return parseResponse(trimmed, resp.body());
                }, ThreadPools.NETWORK_POOL)
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
        JsonElement parsed;
        try {
            parsed = gson.fromJson(body, JsonElement.class);
        } catch (Exception parseEx) {
            return messageResult(sql, "执行响应解析失败: " + safeSnippet(body));
        }
        if (parsed == null || !parsed.isJsonObject()) {
            return messageResult(sql, "执行响应不是合法的 JSON 对象: " + safeSnippet(body));
        }
        JsonObject obj = parsed.getAsJsonObject();
        int rowsCount = 0;
        if (obj.has("rows_count") && !obj.get("rows_count").isJsonNull()) {
            try {
                rowsCount = Integer.parseInt(obj.get("rows_count").getAsString());
            } catch (NumberFormatException ignore) {
                rowsCount = 0;
            }
        }
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
        if (columns.isEmpty() && obj.has("msg")) {
            return messageResult(sql, obj.get("msg").getAsString());
        }
        return new SqlExecResult(sql, columns, rows, rowsCount);
    }

    private SqlExecResult messageResult(String sql, String message) {
        List<String> columns = new ArrayList<>();
        columns.add("消息");
        List<List<String>> rows = new ArrayList<>();
        rows.add(List.of(message));
        return new SqlExecResult(sql, columns, rows, 1);
    }

    private String safeSnippet(String body) {
        if (body == null) {
            return "<空响应>";
        }
        String t = body.strip();
        return t.length() > 240 ? t.substring(0, 240) + "..." : t;
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
