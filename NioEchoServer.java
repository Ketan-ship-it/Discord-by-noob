import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

public class NioEchoServer {

    private static final int PORT = 9001;

    public static void main(String[] args) throws IOException {

        Selector selector = Selector.open();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("NIO Echo Server started on port " + PORT);

        while (true) {

            selector.select();  // blocks until ANY event happens

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (key.isAcceptable()) {
                    acceptConnection(serverChannel, selector);
                }
                
                if (key.isReadable()) {
                    readAndEcho(key);
                }
                System.out.println("Thread name = " + Thread.currentThread().getName());
            }
        }
    }

    private static void acceptConnection(ServerSocketChannel serverChannel, Selector selector)
            throws IOException {

        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        client.register(selector, SelectionKey.OP_READ, buffer);

        System.out.println("Accepted connection: " + client.getRemoteAddress());
    }

    private static void readAndEcho(SelectionKey key) throws IOException {

        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        int bytesRead = client.read(buffer);

        if (bytesRead == -1) {
            client.close();
            return;
        }

        buffer.flip();
        client.write(buffer);
        buffer.clear();
    }
}