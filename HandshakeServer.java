
import gateway.core.EventLoop;

public class HandshakeServer {

    private static final int PORT = 9002;
    private static final long HANDSHAKE_TIMEOUT_MS = 100000;
    private static final long IDLE_TIMEOUT_MS = 300000;

    public static void main(String[] args) throws Exception {

        EventLoop loop = new EventLoop(PORT, IDLE_TIMEOUT_MS, HANDSHAKE_TIMEOUT_MS);
        loop.start();
    }
}