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
import java.net.URLEncoder;
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

        // URL 安全编码，但保留单引号，避免后端对引号的特殊处理导致响应异常
        String encoded = URLEncoder.encode(normalized, StandardCharsets.UTF_8)
                .replace("+", "%20")
                .replace("%27", "'");
        String wrapped = "$$" + encoded + "$$";

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

        // rows_count 仍按接口返回为准
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

        if (data != null && data.size() > 0) {
            // 找到第一条真正的对象行，后面所有“魔法”都围绕它展开
            JsonObject firstRow = null;
            for (JsonElement el : data) {
                if (el != null && el.isJsonObject()) {
                    firstRow = el.getAsJsonObject();
                    break;
                }
            }

            // ========== 1) 优先：使用第一行的 __col_meta 决定列顺序 ==========
            if (firstRow != null && firstRow.has("__col_meta") && !firstRow.get("__col_meta").isJsonNull()) {
                JsonElement metaEl = firstRow.get("__col_meta");
                JsonArray metaArr = null;

                // 兼容两种情况：
                // 1) __col_meta 本身就是 JSON 数组
                // 2) __col_meta 是字符串，里面是 JSON 数组文本
                if (metaEl.isJsonArray()) {
                    metaArr = metaEl.getAsJsonArray();
                } else if (metaEl.isJsonPrimitive() && metaEl.getAsJsonPrimitive().isString()) {
                    try {
                        metaArr = gson.fromJson(metaEl.getAsString(), JsonArray.class);
                    } catch (Exception ignore) {
                        metaArr = null;
                    }
                }

                if (metaArr != null) {
                    class ColMeta {
                        final String name;
                        final int seq;

                        ColMeta(String name, int seq) {
                            this.name = name;
                            this.seq = seq;
                        }
                    }

                    List<ColMeta> metaList = new ArrayList<>();
                    for (JsonElement mEl : metaArr) {
                        if (mEl == null || !mEl.isJsonObject()) {
                            continue;
                        }
                        JsonObject mObj = mEl.getAsJsonObject();
                        if (!mObj.has("col_name") || mObj.get("col_name").isJsonNull()) {
                            continue;
                        }
                        String name = mObj.get("col_name").getAsString();
                        if (name == null || name.isEmpty() || shouldSkip(name)) {
                            continue;
                        }

                        int seq = 0;
                        if (mObj.has("col_seq") && !mObj.get("col_seq").isJsonNull()) {
                            try {
                                seq = mObj.get("col_seq").getAsInt();
                            } catch (Exception ignore) {
                                seq = 0;
                            }
                        }
                        metaList.add(new ColMeta(name, seq));
                    }

                    // 按 col_seq 排序（从 1 开始的自然顺序）
                    metaList.sort((a, b) -> Integer.compare(a.seq, b.seq));

                    for (ColMeta cm : metaList) {
                        if (!columns.contains(cm.name)) {
                            columns.add(cm.name);
                        }
                    }
                }
            }

            // ========== 2) 兜底：没有 __col_meta 或解析失败时的旧逻辑 ==========
            if (columns.isEmpty() && firstRow != null) {
                // (a) 看根对象是否有 "columns" 数组
                if (obj.has("columns") && obj.get("columns").isJsonArray()) {
                    JsonArray colsArr = obj.getAsJsonArray("columns");
                    for (JsonElement colEl : colsArr) {
                        if (colEl == null || colEl.isJsonNull()) continue;
                        String colName = colEl.getAsString();
                        if (shouldSkip(colName)) continue;
                        columns.add(colName);
                    }
                } else {
                    // (b) 否则就按第一行 JSON 字段出现的顺序
                    for (Map.Entry<String, JsonElement> entry : firstRow.entrySet()) {
                        String key = entry.getKey();
                        if (shouldSkip(key)) {
                            continue;
                        }
                        columns.add(key);
                    }
                }
            }

            // ========== 3) 按确定好的列顺序，构造所有行的数据 ==========
            if (!columns.isEmpty()) {
                for (JsonElement el : data) {
                    if (el == null || !el.isJsonObject()) {
                        continue;
                    }
                    JsonObject rowObj = el.getAsJsonObject();
                    List<String> row = new ArrayList<>(columns.size());
                    for (String col : columns) {
                        JsonElement value = rowObj.get(col);
                        if (value == null || value.isJsonNull()) {
                            row.add("");
                        } else {
                            row.add(value.getAsString());
                        }
                    }
                    rows.add(row);
                }
            } else {
                // Data 有内容但没拿到任何有效列的极端情况：
                // 把整行 JSON 当成一列文本交给前端，至少不至于全空。
                for (JsonElement el : data) {
                    String text = (el == null || el.isJsonNull()) ? "" : el.toString();
                    List<String> row = new ArrayList<>();
                    row.add(text);
                    rows.add(row);
                }
                columns.add("结果");
            }
        }

        // 如果没有任何列，但有 msg，就当成提示消息
        if (columns.isEmpty() && obj.has("msg") && !obj.get("msg").isJsonNull()) {
            return messageResult(sql, obj.get("msg").getAsString());
        }

        // 再兜个底：什么都没有，就给一条“无数据返回”的提示
        if (columns.isEmpty()) {
            return messageResult(sql, "无数据返回");
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
