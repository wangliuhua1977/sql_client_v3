package tools.sqlclient.exec;

import java.net.URI;

/**
 * 异步 SQL 执行相关的默认配置。
 */
public class AsyncSqlConfig {
    public static final String BASE_URL = "https://leshan.paas.sc.ctc.com/waf/api";
    public static final String REQUEST_TOKEN = "WAF_STATIC_TOKEN_202405";
    public static final String AES_KEY = "LeshanAESKey1234";
    public static final String AES_IV = "LeshanAESIv12345";

    private AsyncSqlConfig() {
    }

    public static URI buildUri(String path) {
        String base = BASE_URL.endsWith("/") ? BASE_URL.substring(0, BASE_URL.length() - 1) : BASE_URL;
        String normalized = path.startsWith("/") ? path : "/" + path;
        return URI.create(base + normalized);
    }
}
