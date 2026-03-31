package gateway.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
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

    private static final boolean DEBUG = true;
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

        if (conn.state == ConnectionState.AWAITING_HEADERS) {
            
            int bytesRead = client.read(conn.buffer);

            conn.lastActivity = System.currentTimeMillis();

            if (bytesRead == -1) {
                if (conn.state == ConnectionState.AWAITING_HEADERS) {
                    failedHandshakes.incrementAndGet();
                }
                close(key);
                return;
            }

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
        }else if(conn.state == ConnectionState.UPGRADED){
            int read = client.read(conn.frameBuffer);

            conn.lastActivity = System.currentTimeMillis();

            if(read == -1){
                close(key);
                return;
            }

            conn.frameBuffer.flip();

            while(parseFrames(conn , client , key)){
            }

            conn.frameBuffer.compact();
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

                // sendHello(client);
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

            if(DEBUG){
                System.out.println("Conn lastActivity: " + conn.lastActivity +
                           " | now: " + now +
                           " | diff: " + (now - conn.lastActivity));
            }

            if (conn.state == ConnectionState.AWAITING_HEADERS &&
                now - conn.startTime > HANDSHAKE_TIMEOUT_MS) {

                failedHandshakes.incrementAndGet();
                if(DEBUG){
                    System.out.println("Closing connection due to handshake timeout");
                }
                close(key);

            } else if (conn.state == ConnectionState.UPGRADED &&
                       now - conn.lastActivity > IDLE_TIMEOUT_MS) {

                if(DEBUG){
                    System.out.println("Closing connection due to idle timeout");
                }
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

    private static boolean parseFrames(Connection conn , SocketChannel client , SelectionKey key) throws IOException{

        ByteBuffer buff = conn.frameBuffer;

        if(buff.remaining() < 2) return false;

        buff.mark();

        byte b1 = buff.get();
        byte b2 = buff.get();

        boolean fin = (b1 & 0x80) != 0;
        int opcode = b1 & 0x0F;

        if(DEBUG){
            System.out.println("Received frame opcode: " + opcode);
        }

        boolean masked = (b2 & 0x80) != 0;
        int payloadLen = b2 & 0x7F;

        // TODO: fragmentation not supported yet
        /*
            You’re reading FIN, but ignoring it means you assume “1 frame = 1 message” — which is usually true, but not guaranteed.
            Adding fragmentation support is a bit more complex, but it’s the only way to be fully compliant with the spec. 
            So it is ignored for now, but should be implemented in the future to handle edge cases and ensure compatibility with all clients.
        */
        if (!fin) {
            return true;
        }

        if(payloadLen == 126){
            if(buff.remaining() < 2){
                buff.reset();
                return false;
            }
            payloadLen = (int) buff.getShort() & 0xFFFF;
        }else if(payloadLen == 127){
            if (buff.remaining() < 8) {
                buff.reset();
                return false;
            }
            payloadLen = (int) buff.getLong();
        }

        byte[] maskingKey = null;

        if(masked){
            if(buff.remaining()<4){
                buff.reset();
                return false;
            }
            maskingKey = new byte[4];
            buff.get(maskingKey);
        }

        if(buff.remaining() < payloadLen){
            buff.reset();
            return false;
        }

        byte[] payload = new byte[payloadLen];
        buff.get(payload);

        if(masked){
            for(int i=0;i<payloadLen;i++){
                payload[i]^=maskingKey[i%4];
            }
        }

        handleFrames(conn , client , key , opcode , payload );

        return true;
    }

    private static void handleFrames(Connection conn , SocketChannel client ,SelectionKey key, int opcode , byte[]payload) throws IOException{

        switch (opcode){

            case 0x1: 
                //text frame
                String message = new String(payload);
                if(DEBUG){
                    System.out.println("Received message: " + message);
                }

                sendText(conn , client , message);
                break;

            case 0X8:
                //close frame
                //client.close(); This causes issues because the selector still has the key registered, so we need to close it through the event loop to ensure proper cleanup and avoid exceptions.
                close(key);
                break;
                
            case 0x9:
                // ping frame
                sendPong(conn , client);
                break;
        }
    }

    private static void sendText(Connection conn ,SocketChannel client , String message) throws IOException{

        conn.lastActivity = System.currentTimeMillis(); // ✅ Missing this caused connections to be closed after handshake because the idle timeout thought they were inactive. Now we update lastActivity whenever we send a message to ensure the connection stays alive as long as there is activity.

        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        int len = data.length;

        ByteBuffer frame;

        if(len <= 125){
            frame = ByteBuffer.allocate(2 + len);
            frame.put((byte)0x81);
            frame.put((byte)len);
        }else if(len <= 65535){
            frame = ByteBuffer.allocate(4 + len);
            frame.put((byte)0x81);
            frame.put((byte)126);
            frame.putShort((short)len);
        }else{
            frame = ByteBuffer.allocate(10 + len);
            frame.put((byte)0x81);
            frame.put((byte)127);
            frame.putLong(len);
        }

        frame.put(data);
        frame.flip();

        client.write(frame);

    }

    private static void sendPong(Connection conn , SocketChannel client)
        throws IOException {

        conn.lastActivity = System.currentTimeMillis(); // ✅ Missing this caused connections to be closed after handshake because the idle timeout thought they were inactive. Now we update lastActivity whenever we send a message to ensure the connection stays alive as long as there is activity.
        ByteBuffer frame = ByteBuffer.allocate(2);

        frame.put((byte)0x8A);
        frame.put((byte)0);

        frame.flip();

        client.write(frame);
    }

    private static void sendHello(Connection conn ,SocketChannel client) throws IOException{

        ByteBuffer on_handshake_instructions = ByteBuffer.allocate(1+4);

        on_handshake_instructions.put((byte)10);  //opcode for "hello" message
        on_handshake_instructions.putInt(3000); //heartbeat interval in ms

        on_handshake_instructions.flip();

        if (DEBUG) {
            System.out.println("Sending hello message");
        }

        sendBinaryFrame(conn , client , on_handshake_instructions);
        
    }

    private static void sendBinaryFrame(Connection conn , SocketChannel client , ByteBuffer on_handshake_instructions) throws IOException{

        conn.lastActivity = System.currentTimeMillis(); // ✅ Missing this caused connections to be closed after handshake because the idle timeout thought they were inactive. Now we update lastActivity whenever we send a message to ensure the connection stays alive as long as there is activity.
        int len = on_handshake_instructions.remaining();

        ByteBuffer frame = ByteBuffer.allocate(2 + len);
        frame.put((byte)0x82); //binary frame opcode

        if(len <= 125){
            frame.put((byte)len);
        }else{
            throw new IllegalArgumentException("Payload too large for now");
        }

        frame.put(on_handshake_instructions);
        frame.flip();

        client.write(frame);

    }

}