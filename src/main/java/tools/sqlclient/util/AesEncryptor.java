package tools.sqlclient.util;

import tools.sqlclient.exec.AsyncSqlConfig;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * AES/CBC/PKCS5Padding 加密工具，用于异步 SQL 提交。
 */
public class AesEncryptor {
    private AesEncryptor() {
    }

    public static String encryptSqlToBase64(String plainSql) {
        try {
            byte[] keyBytes = AsyncSqlConfig.AES_KEY.getBytes(StandardCharsets.UTF_8);
            byte[] ivBytes = AsyncSqlConfig.AES_IV.getBytes(StandardCharsets.UTF_8);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(ivBytes);
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            byte[] encrypted = cipher.doFinal(plainSql.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new IllegalStateException("SQL 加密失败", e);
        }
    }
}
