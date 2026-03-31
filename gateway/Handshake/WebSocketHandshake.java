package gateway.Handshake;

import java.security.MessageDigest;
import java.util.Base64;

public class WebSocketHandshake {

    private static final String WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private static final byte[] WS_MAGIC_BYTES = WS_MAGIC.getBytes();

    public static String computeAccept(byte[] keyBytes)
            throws Exception {

        byte[] combined =
                new byte[keyBytes.length + WS_MAGIC_BYTES.length];

        System.arraycopy(keyBytes, 0, combined, 0, keyBytes.length);
        System.arraycopy(WS_MAGIC_BYTES, 0, combined,
                keyBytes.length, WS_MAGIC_BYTES.length);

        MessageDigest sha1 =
                MessageDigest.getInstance("SHA-1");

        byte[] hash = sha1.digest(combined);

        return Base64
                .getEncoder()
                .encodeToString(hash);
    }
}