package tools.sqlclient.exec;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * AES/CBC/PKCS5Padding + Base64 工具，用于异步 SQL 提交。
 */
public class AesCbcBase64 {
    private AesCbcBase64() {
    }

    public static String encrypt(String plainText) {
        try {
            byte[] keyBytes = AsyncSqlConfig.AES_KEY.getBytes(StandardCharsets.UTF_8);
            byte[] ivBytes = AsyncSqlConfig.AES_IV.getBytes(StandardCharsets.UTF_8);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(ivBytes);
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new IllegalStateException("AES 加密失败", e);
        }
    }
}
