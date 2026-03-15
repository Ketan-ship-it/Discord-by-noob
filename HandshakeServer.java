import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

public class HandshakeServer {

    private static final int PORT = 9002;
    private static final int BUFFER_SIZE = 8192;
    private static final long HANDSHAKE_TIMEOUT_MS = 10000;

    private static final byte[] HTTP_OK =
            ("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                    .getBytes();

    private static final String WS_MAGIC =
            "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private static final ExecutorService virtualExecutor =
            Executors.newVirtualThreadPerTaskExecutor();

    enum State {
        AWAITING_HEADERS,
        WRITING_RESPONSE,
        UPGRADED,
        CLOSED
    }

    static class Connection {
        ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        State state = State.AWAITING_HEADERS;
        long startTime = System.currentTimeMillis();
        ByteBuffer writeBuffer;
    }

    public static void main(String[] args) throws Exception {

        Selector selector = Selector.open();

        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(PORT));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Handshake server running on port " + PORT);

        while (true) {

            selector.select(1000);
            cleanupTimeouts(selector);

            Iterator<SelectionKey> keys =
                    selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) continue;

try {

    if (key.isAcceptable()) {
        accept(selector, server);
    }

    if (key.isReadable()) {
        read(key, selector);
    }

    if (key.isWritable()) {
        write(key);
    }

} catch (CancelledKeyException ignored) {
}
            }
        }
    }

    private static void accept(Selector selector,
                               ServerSocketChannel server)
            throws IOException {

        SocketChannel client = server.accept();
        client.configureBlocking(false);

        Connection conn = new Connection();
        client.register(selector,
                SelectionKey.OP_READ, conn);

        System.out.println("Accepted: " +
                client.getRemoteAddress());
    }

    private static void read(SelectionKey key,
                             Selector selector)
            throws IOException {

        SocketChannel client =
                (SocketChannel) key.channel();
        Connection conn =
                (Connection) key.attachment();

        int bytesRead = client.read(conn.buffer);

        if (bytesRead == -1) {
            close(key);
            return;
        }

        if (conn.state == State.AWAITING_HEADERS) {

            if (headersComplete(conn.buffer)) {

                String request =
                        extractRequest(conn.buffer);
                        System.out.println("----REQUEST----");
System.out.println(request);
System.out.println("---------------");

                String lower = request.toLowerCase();

if (lower.contains("upgrade: websocket")
        && lower.contains("sec-websocket-key")) {

                    String wsKey = extractWebSocketKey(request);

if (wsKey == null) {
    close(key);
    return;
}
                    try {

    String accept = computeAccept(wsKey);

    String response =
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: " +
            accept + "\r\n\r\n";

    conn.writeBuffer = ByteBuffer.wrap(response.getBytes());
    conn.state = State.WRITING_RESPONSE;

} catch (Exception e) {
    e.printStackTrace();
}
                } else {
                    conn.writeBuffer =
                            ByteBuffer.wrap(HTTP_OK);
                    conn.state =
                            State.WRITING_RESPONSE;
                }

                key.interestOps(SelectionKey.OP_WRITE);
            }
        }
    }

    private static void write(SelectionKey key)
            throws IOException {

        SocketChannel client =
                (SocketChannel) key.channel();
        Connection conn =
                (Connection) key.attachment();

        client.write(conn.writeBuffer);

        if (!conn.writeBuffer.hasRemaining()) {

            if (conn.state == State.WRITING_RESPONSE) {
                conn.state = State.UPGRADED;
                conn.buffer.clear();
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

private static boolean headersComplete(ByteBuffer buffer) {

    buffer.flip();

    for (int i = 0; i < buffer.limit() - 3; i++) {
        if (buffer.get(i) == '\r' &&
            buffer.get(i + 1) == '\n' &&
            buffer.get(i + 2) == '\r' &&
            buffer.get(i + 3) == '\n') {

            buffer.position(buffer.limit());
            buffer.limit(buffer.capacity());
            return true;
        }
    }

    buffer.compact();
    return false;
}
    private static String extractRequest(ByteBuffer buffer) {

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.clear();

        return new String(bytes);
    }

    private static String extractWebSocketKey(String request) {

    for (String line : request.split("\r\n")) {

        String lower = line.toLowerCase();

        if (lower.startsWith("sec-websocket-key:")) {
            return line.substring(line.indexOf(":") + 1).trim();
        }
    }

    return null;
}

    private static String computeAccept(String key)
            throws Exception {

        String combined = key + WS_MAGIC;

        MessageDigest sha1 =
                MessageDigest.getInstance("SHA-1");

        byte[] hash =
                sha1.digest(combined.getBytes());

        return Base64.getEncoder()
                .encodeToString(hash);
    }

    private static void cleanupTimeouts(Selector selector) {

        long now = System.currentTimeMillis();

        for (SelectionKey key : selector.keys()) {

            if (!(key.attachment() instanceof Connection))
                continue;

            Connection conn =
                    (Connection) key.attachment();

            if (conn.state ==
                    State.AWAITING_HEADERS &&
                now - conn.startTime >
                        HANDSHAKE_TIMEOUT_MS) {

                close(key);
            }
        }
    }

    private static void close(SelectionKey key) {
        try {
            key.channel().close();
        } catch (IOException ignored) {}
        key.cancel();
    }
}
