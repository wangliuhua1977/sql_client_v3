package tools.sqlclient.metadata;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.db.SQLiteManager;
import tools.sqlclient.network.TrustAllHttpClient;

import javax.swing.tree.DefaultMutableTreeNode;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * 管理元数据：差分更新、本地缓存、模糊匹配。
 */
public class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    private static final String OBJ_API = "https://leshan.paas.sc.ctc.com/waf-dev/api?api_id=sql_obj&api_token=jtc3r&p_type=1";
    private static final String COL_API_TEMPLATE = "https://leshan.paas.sc.ctc.com/waf-dev/api?api_id=sql_obj&api_token=jtc3r&p_type=2&p_obj=%s";

    private final SQLiteManager sqliteManager;
    private final ExecutorService metadataPool = Executors.newFixedThreadPool(4, r -> new Thread(r, "metadata-pool"));
    private final ExecutorService networkPool = Executors.newFixedThreadPool(6, r -> new Thread(r, "network-pool"));
    private final Gson gson = new Gson();
    private final HttpClient httpClient = TrustAllHttpClient.create();

    public MetadataService(Path dbPath) {
        this.sqliteManager = new SQLiteManager(dbPath);
        this.sqliteManager.initSchema();
    }

    public void refreshMetadataAsync(Runnable done) {
        CompletableFuture.runAsync(this::refreshMetadata, metadataPool).thenRunAsync(done);
    }

    private void refreshMetadata() {
        try {
            List<RemoteObject> remoteObjects = fetchObjects();
            Set<String> remoteNames = remoteObjects.stream().map(o -> o.object_name).collect(Collectors.toSet());

            try (Connection conn = sqliteManager.getConnection()) {
                conn.setAutoCommit(false);
                Set<String> existing = new HashSet<>();
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT object_name FROM objects")) {
                    while (rs.next()) {
                        existing.add(rs.getString(1));
                    }
                }
                // 插入新增
                try (PreparedStatement ps = conn.prepareStatement("INSERT OR IGNORE INTO objects(schema_name, object_name, object_type) VALUES(?,?,?)")) {
                    for (RemoteObject obj : remoteObjects) {
                        ps.setString(1, obj.schema_name);
                        ps.setString(2, obj.object_name);
                        ps.setString(3, obj.object_type);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                // 删除不存在（可扩展: 通过配置控制，这里默认删除）
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM objects WHERE object_name = ?")) {
                    for (String old : existing) {
                        if (!remoteNames.contains(old)) {
                            ps.setString(1, old);
                            ps.addBatch();
                        }
                    }
                    ps.executeBatch();
                }
                conn.commit();
            }

            // 更新列信息
            List<CompletableFuture<Void>> tasks = new ArrayList<>();
            for (RemoteObject obj : remoteObjects) {
                if (obj.object_type.equals("table") || obj.object_type.equals("view")) {
                    tasks.add(CompletableFuture.runAsync(() -> updateColumns(obj.object_name), networkPool));
                }
            }
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("刷新元数据失败", e);
        }
    }

    private List<RemoteObject> fetchObjects() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(OBJ_API)).GET().build();
        HttpResponse<java.io.InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        Type listType = new TypeToken<List<RemoteObject>>(){}.getType();
        try (InputStreamReader reader = new InputStreamReader(response.body(), StandardCharsets.UTF_8)) {
            JsonElement root = gson.fromJson(reader, JsonElement.class);
            return parseListFromJson(root, listType);
        }
    }

    private void updateColumns(String objectName) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(COL_API_TEMPLATE.formatted(objectName))).GET().build();
            HttpResponse<java.io.InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            Type listType = new TypeToken<List<RemoteColumn>>(){}.getType();
            List<RemoteColumn> columns;
            try (InputStreamReader reader = new InputStreamReader(response.body(), StandardCharsets.UTF_8)) {
                JsonElement root = gson.fromJson(reader, JsonElement.class);
                columns = parseListFromJson(root, listType);
            }
            if (columns == null) return;
            try (Connection conn = sqliteManager.getConnection()) {
                conn.setAutoCommit(false);
                List<String> localCols = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement("SELECT column_name FROM columns WHERE object_name=?")) {
                    ps.setString(1, objectName);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) localCols.add(rs.getString(1));
                    }
                }
                boolean changed = columns.size() != localCols.size() ||
                        !new HashSet<>(localCols).containsAll(columns.stream().map(c -> c.column_name).toList());
                if (changed) {
                    try (PreparedStatement del = conn.prepareStatement("DELETE FROM columns WHERE object_name=?")) {
                        del.setString(1, objectName);
                        del.executeUpdate();
                    }
                    try (PreparedStatement ins = conn.prepareStatement("INSERT INTO columns(schema_name, object_name, column_name, sort_no) VALUES(?,?,?,?)")) {
                        int i = 0;
                        for (RemoteColumn col : columns) {
                            ins.setString(1, col.schema_name);
                            ins.setString(2, objectName);
                            ins.setString(3, col.column_name);
                            ins.setInt(4, i++);
                            ins.addBatch();
                        }
                        ins.executeBatch();
                    }
                    conn.commit();
                }
            }
        } catch (Exception e) {
            log.error("更新字段失败: {}", objectName, e);
        }
    }

    public void loadTree(DefaultMutableTreeNode root) {
        try (Connection conn = sqliteManager.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT object_type, object_name FROM objects ORDER BY object_type, object_name")) {
            Map<String, DefaultMutableTreeNode> typeNodes = new LinkedHashMap<>();
            while (rs.next()) {
                String type = rs.getString("object_type");
                String name = rs.getString("object_name");
                DefaultMutableTreeNode typeNode = typeNodes.computeIfAbsent(type, DefaultMutableTreeNode::new);
                typeNode.add(new DefaultMutableTreeNode(name));
            }
            typeNodes.values().forEach(root::add);
        } catch (Exception e) {
            log.error("加载树失败", e);
        }
    }

    public List<String> fuzzyMatch(String token, int limit) {
        String[] keywords = token.toLowerCase().split("%");
        String likeSql = Arrays.stream(keywords).map(k -> "%" + k + "%").collect(Collectors.joining());
        String sql = "SELECT object_name FROM objects WHERE lower(object_name) LIKE ? ORDER BY use_count DESC, object_name ASC LIMIT ?";
        try (Connection conn = sqliteManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, likeSql);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> list = new ArrayList<>();
                while (rs.next()) list.add(rs.getString(1));
                return list;
            }
        } catch (Exception e) {
            log.error("模糊匹配失败", e);
            return List.of();
        }
    }

    /**
     * 兼容 API 返回值既可能是数组，也可能是包装对象（如 {"data": [...]}）。
     */
    private <T> List<T> parseListFromJson(JsonElement root, Type listType) {
        if (root == null || root.isJsonNull()) {
            return List.of();
        }
        if (root.isJsonArray()) {
            return gson.fromJson(root, listType);
        }
        if (root.isJsonObject()) {
            JsonObject obj = root.getAsJsonObject();
            for (String key : List.of("Data", "data", "rows", "list", "items", "result")) {
                JsonElement maybeArray = obj.get(key);
                if (maybeArray != null && maybeArray.isJsonArray()) {
                    JsonArray arr = maybeArray.getAsJsonArray();
                    return gson.fromJson(arr, listType);
                }
            }
        }
        // fallback: 返回空列表，避免抛出解析异常
        log.warn("未能从 JSON 解析出数组结构: {}", root);
        return List.of();
    }

    static class RemoteObject {
        String schema_name;
        String object_name;
        String object_type;
    }

    static class RemoteColumn {
        String schema_name;
        String object_name;
        String column_name;
    }
}
