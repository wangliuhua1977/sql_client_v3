package tools.sqlclient.remote;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AesCbcTest {

    @Test
    void shouldEncryptAndDecryptRoundTrip() {
        String sql = "SELECT * FROM demo WHERE id = 42 AND note = '测试'";
        String cipher = AesCbc.encrypt(sql);
        assertNotNull(cipher);
        assertFalse(cipher.isBlank());
        assertEquals(cipher, AesCbc.encrypt(sql), "同一明文在固定 key/iv 下应生成稳定密文");
        String plain = AesCbc.decrypt(cipher);
        assertEquals(sql, plain);
    }
}
