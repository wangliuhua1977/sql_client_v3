package tools.sqlclient.remote;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * AES/CBC/PKCS5Padding + Base64 工具。
 */
public final class AesCbc {
    private AesCbc() {
    }

    public static String encrypt(String plainText) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, buildKey(), buildIv());
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new IllegalStateException("AES 加密失败", e);
        }
    }

    public static String decrypt(String base64CipherText) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, buildKey(), buildIv());
            byte[] decoded = Base64.getDecoder().decode(base64CipherText);
            byte[] plain = cipher.doFinal(decoded);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("AES 解密失败", e);
        }
    }

    private static SecretKeySpec buildKey() {
        byte[] keyBytes = RemoteSqlConfig.AES_KEY.getBytes(StandardCharsets.UTF_8);
        return new SecretKeySpec(keyBytes, "AES");
    }

    private static IvParameterSpec buildIv() {
        byte[] ivBytes = RemoteSqlConfig.AES_IV.getBytes(StandardCharsets.UTF_8);
        return new IvParameterSpec(ivBytes);
    }
}
