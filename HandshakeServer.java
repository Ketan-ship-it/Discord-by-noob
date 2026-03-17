import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class HandshakeServer {

    private static final int PORT = 9002;
    private static final int BUFFER_SIZE = 8192;
    private static final long HANDSHAKE_TIMEOUT_MS = 10000;
    private static final long IDLE_TIMEOUT_MS = 30000;

    private static final String WS_MAGIC =
            "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private static final byte[] WS_MAGIC_BYTES =
            WS_MAGIC.getBytes();

    private static final ExecutorService virtualExecutor =
            Executors.newVirtualThreadPerTaskExecutor();

    private static final ConcurrentLinkedQueue<Runnable> pendingTasks =
            new ConcurrentLinkedQueue<>();

    private static final AtomicLong totalConnections = new AtomicLong();
    private static final AtomicLong activeConnections = new AtomicLong();
    private static final AtomicLong completedHandshakes = new AtomicLong();
    private static final AtomicLong failedHandshakes = new AtomicLong();

    // selector latency metrics (windowed)
    private static long lastLoopStart = System.nanoTime();
    private static long windowLoops = 0;
    private static long windowLatencyTotal = 0;
    private static long windowMaxLatency = 0;

    private static final boolean DEBUG = false;

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
        long lastActivity = System.currentTimeMillis();
        ByteBuffer writeBuffer;
        boolean countedClosed = false;
    }

    public static void main(String[] args) throws Exception {

        Selector selector = Selector.open();

        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(PORT));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Handshake server running on port " + PORT);

        startMetricsPrinter();

        while (true) {

            long now = System.nanoTime();
            long latencyMicros = (now - lastLoopStart) / 1000;
            lastLoopStart = now;

            windowLoops++;
            windowLatencyTotal += latencyMicros;

            if (latencyMicros > windowMaxLatency) {
                windowMaxLatency = latencyMicros;
            }

            selector.select(1000);

            Runnable task;
            while ((task = pendingTasks.poll()) != null) {
                task.run();
            }

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

    private static void accept(
            Selector selector,
            ServerSocketChannel server)
            throws IOException {

        SocketChannel client = server.accept();
        client.configureBlocking(false);

        Connection conn = new Connection();

        client.register(selector, SelectionKey.OP_READ, conn);

        totalConnections.incrementAndGet();
        activeConnections.incrementAndGet();

        if (DEBUG) {
            System.out.println("Accepted: " + client.getRemoteAddress());
        }
    }

    private static void read(
            SelectionKey key,
            Selector selector)
            throws IOException {

        SocketChannel client = (SocketChannel) key.channel();
        Connection conn = (Connection) key.attachment();

        int bytesRead = client.read(conn.buffer);

        conn.lastActivity = System.currentTimeMillis();

        if (bytesRead == -1) {
            if (conn.state == State.AWAITING_HEADERS) {
                failedHandshakes.incrementAndGet();
            }
            close(key);
            return;
        }

        if (conn.state == State.AWAITING_HEADERS) {

            if (headersComplete(conn.buffer)) {

                conn.buffer.flip();

                int headerEnd = findHeaderEnd(conn.buffer);

                if (headerEnd == -1) {
                    failedHandshakes.incrementAndGet();
                    close(key);
                    return;
                }

                byte[] wsKeyBytes =
                        extractWebSocketKeyBytes(conn.buffer, headerEnd);

                conn.buffer.clear();

                if (wsKeyBytes == null || wsKeyBytes.length == 0) {
                    failedHandshakes.incrementAndGet();
                    close(key);
                    return;
                }

                SelectionKey selectionKey = key;

                virtualExecutor.submit(() -> {

                    try {

                        String accept = computeAccept(wsKeyBytes);

                        String response =
                                "HTTP/1.1 101 Switching Protocols\r\n" +
                                "Upgrade: websocket\r\n" +
                                "Connection: Upgrade\r\n" +
                                "Sec-WebSocket-Accept: " +
                                accept + "\r\n\r\n";

                        ByteBuffer responseBuffer =
                                ByteBuffer.wrap(response.getBytes());

                        pendingTasks.offer(() -> {

                            conn.writeBuffer = responseBuffer;
                            conn.state = State.WRITING_RESPONSE;

                            selectionKey.interestOps(
                                    SelectionKey.OP_WRITE);

                        });

                        selector.wakeup();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private static void write(SelectionKey key)
            throws IOException {

        SocketChannel client = (SocketChannel) key.channel();
        Connection conn = (Connection) key.attachment();

        if (conn.writeBuffer == null) return;

        client.write(conn.writeBuffer);

        if (!conn.writeBuffer.hasRemaining()) {

            if (conn.state == State.WRITING_RESPONSE) {

                conn.state = State.UPGRADED;

                completedHandshakes.incrementAndGet();

                conn.buffer.clear();

                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    private static boolean headersComplete(ByteBuffer buffer) {

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

    private static String computeAccept(byte[] keyBytes)
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

    private static void cleanupTimeouts(Selector selector) {

        long now = System.currentTimeMillis();

        for (SelectionKey key : selector.keys()) {

            if (!(key.attachment() instanceof Connection))
                continue;

            Connection conn =
                    (Connection) key.attachment();

            if (conn.state == State.AWAITING_HEADERS &&
                now - conn.startTime > HANDSHAKE_TIMEOUT_MS) {

                failedHandshakes.incrementAndGet();
                close(key);

            } else if (conn.state == State.UPGRADED &&
                       now - conn.lastActivity > IDLE_TIMEOUT_MS) {

                close(key);
            }
        }
    }

    private static void close(SelectionKey key) {

        Connection conn = (Connection) key.attachment();

        if (conn != null && !conn.countedClosed) {

            conn.countedClosed = true;
            activeConnections.decrementAndGet();
        }

        try {
            key.channel().close();
        } catch (IOException ignored) {}

        key.cancel();
    }

    private static int findHeaderEnd(ByteBuffer buffer) {

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

    private static byte[] extractWebSocketKeyBytes(
            ByteBuffer buffer,
            int headerEnd) {

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

    private static long getDirectMemoryUsage() {

        for (BufferPoolMXBean pool :
                ManagementFactory.getPlatformMXBeans(
                        BufferPoolMXBean.class)) {

            if ("direct".equals(pool.getName())) {
                return pool.getMemoryUsed();
            }
        }

        return 0;
    }

    private static void startMetricsPrinter() {

        ScheduledExecutorService metrics =
                Executors.newSingleThreadScheduledExecutor();

        metrics.scheduleAtFixedRate(() -> {

            long avgLatency =
                    windowLoops == 0 ? 0 :
                    windowLatencyTotal / windowLoops;

            long directMemory = getDirectMemoryUsage();

            System.out.println("\n=== Gateway Metrics ===");

            System.out.println("Active connections: " +
                    activeConnections.get());

            System.out.println("Total connections: " +
                    totalConnections.get());

            System.out.println("Completed handshakes: " +
                    completedHandshakes.get());

            System.out.println("Failed handshakes: " +
                    failedHandshakes.get());

            System.out.println("Selector avg latency (µs): " +
                    avgLatency);

            System.out.println("Selector max latency (µs): " +
                    windowMaxLatency);

            System.out.println("Direct memory used: " +
                    (directMemory / 1024 / 1024) + " MB");

            System.out.println("=======================\n");

            // reset window
            windowLoops = 0;
            windowLatencyTotal = 0;
            windowMaxLatency = 0;

        }, 5, 5, TimeUnit.SECONDS);
    }
}