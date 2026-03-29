package gateway.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import gateway.Handshake.WebSocketHandshake;
import gateway.Utils.BufferUtils;
import gateway.Metrics.MetricsCollector;

public class EventLoop {

    // This class will manage the main event loop, handling new connections, reading data, and writing responses.
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
    private static int port;
    private static long HANDSHAKE_TIMEOUT_MS ;
    private static long IDLE_TIMEOUT_MS ;

    public EventLoop(int port , long handshakeTimeout, long idleTimeout) throws Exception {
        EventLoop.port = port;
        EventLoop.HANDSHAKE_TIMEOUT_MS = handshakeTimeout;
        EventLoop.IDLE_TIMEOUT_MS = idleTimeout;
    }

    public void start() throws Exception {

        Selector selector = Selector.open();

        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(port));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Handshake server running on port " + port);

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
            if (conn.state == ConnectionState.AWAITING_HEADERS) {
                failedHandshakes.incrementAndGet();
            }
            close(key);
            return;
        }

        if (conn.state == ConnectionState.AWAITING_HEADERS) {

            if (BufferUtils.headersComplete(conn.buffer)) {

                conn.buffer.flip();

                int headerEnd = BufferUtils.findHeaderEnd(conn.buffer);

                if (headerEnd == -1) {
                    failedHandshakes.incrementAndGet();
                    close(key);
                    return;
                }

                byte[] wsKeyBytes =
                        BufferUtils.extractWebSocketKeyBytes(conn.buffer, headerEnd);

                conn.buffer.clear();

                if (wsKeyBytes == null || wsKeyBytes.length == 0) {
                    failedHandshakes.incrementAndGet();
                    close(key);
                    return;
                }

                SelectionKey selectionKey = key;

                virtualExecutor.submit(() -> {

                    try {

                        String accept = WebSocketHandshake.computeAccept(wsKeyBytes);

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
                            conn.state = ConnectionState.WRITING_RESPONSE;

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

            if (conn.state == ConnectionState.WRITING_RESPONSE) {

                conn.state = ConnectionState.UPGRADED;

                completedHandshakes.incrementAndGet();

                conn.buffer.clear();

                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    private static void cleanupTimeouts(Selector selector) {

        long now = System.currentTimeMillis();

        for (SelectionKey key : selector.keys()) {

            if (!(key.attachment() instanceof Connection))
                continue;

            Connection conn =
                    (Connection) key.attachment();

            if (conn.state == ConnectionState.AWAITING_HEADERS &&
                now - conn.startTime > HANDSHAKE_TIMEOUT_MS) {

                failedHandshakes.incrementAndGet();
                close(key);

            } else if (conn.state == ConnectionState.UPGRADED &&
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

    private static void startMetricsPrinter() {

        ScheduledExecutorService metrics =
                Executors.newSingleThreadScheduledExecutor();

        metrics.scheduleAtFixedRate(() -> {

            long avgLatency =
                    windowLoops == 0 ? 0 :
                    windowLatencyTotal / windowLoops;

            long directMemory = MetricsCollector.getDirectMemoryUsage();

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