package tools.sqlclient.network;

import javax.net.ssl.*;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.http.HttpClient;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * 局域网环境：创建信任所有证书的 HttpClient，并关闭 Hostname 校验。
 * 请勿在生产环境使用。
 */
public class TrustAllHttpClient {
    public static HttpClient create() {
        return create(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
    }

    public static HttpClient create(CookieManager cookieManager) {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {return new X509Certificate[0];}
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                    }
            };
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            HttpClient.Builder builder = HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .sslParameters(new SSLParameters() {{setEndpointIdentificationAlgorithm(null);}});
            if (cookieManager != null) {
                builder.cookieHandler(cookieManager);
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("无法创建信任所有证书的 HttpClient", e);
        }
    }
}
