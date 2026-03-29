package gateway.core;

import java.nio.ByteBuffer;

public class Connection {

    private static final int BUFFER_SIZE = 8192;

    public ByteBuffer frameBuffer;

    public ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    public ConnectionState state = ConnectionState.AWAITING_HEADERS;
    public long startTime = System.currentTimeMillis();
    public long lastActivity = System.currentTimeMillis();
    public ByteBuffer writeBuffer;
    public boolean countedClosed = false;
}