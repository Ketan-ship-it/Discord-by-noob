package gateway.Utils;

import java.nio.ByteBuffer;

public class BufferUtils {

    public static byte[] extractWebSocketKeyBytes(ByteBuffer buffer,int headerEnd) {

        byte[] target = "Sec-WebSocket-Key:".getBytes();

        for (int i = 0; i < headerEnd - target.length; i++) {

            boolean match = true;

            for (int j = 0; j < target.length; j++) {

                if (buffer.get(i + j) != target[j]) {
                    match = false;
                    break;
                }
            }

            if (match) {

                int start = i + target.length;

                while (buffer.get(start) == ' ') start++;

                int end = start;

                while (buffer.get(end) != '\r') end++;

                byte[] keyBytes = new byte[end - start];

                buffer.position(start);
                buffer.get(keyBytes);

                return keyBytes;
            }
        }

        return null;
    }

    public static int findHeaderEnd(ByteBuffer buffer) {

        for (int i = 0; i < buffer.limit() - 3; i++) {

            if (buffer.get(i) == '\r' &&
                buffer.get(i + 1) == '\n' &&
                buffer.get(i + 2) == '\r' &&
                buffer.get(i + 3) == '\n') {

                return i + 4;
            }
        }

        return -1;
    }

    public static boolean headersComplete(ByteBuffer buffer) {

        int limit = buffer.position();

        for (int i = 0; i < limit - 3; i++) {

            if (buffer.get(i) == '\r' &&
                buffer.get(i + 1) == '\n' &&
                buffer.get(i + 2) == '\r' &&
                buffer.get(i + 3) == '\n') {

                return true;
            }
        }

        return false;
    }

}