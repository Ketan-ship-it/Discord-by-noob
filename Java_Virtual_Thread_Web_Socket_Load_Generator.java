import java.io.*;
import java.net.Socket;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Java_Virtual_Thread_Web_Socket_Load_Generator {
    
    private static final String HOST = "10.114.190.72";
    private static final int PORT = 9002;

    private static final int TOTAL_CONNECTIONS = 5000;
    private static final int BATCH_SIZE = 200;

    private static final AtomicInteger success = new AtomicInteger();
    private static final AtomicInteger failed = new AtomicInteger();

    public static void main(String[] args) throws Exception {

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_CONNECTIONS; i++) {

            executor.submit(() -> connect());

            if (i % BATCH_SIZE == 0) {
                System.out.println("Launched: " + i);
                Thread.sleep(200);
            }
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);

        long end = System.currentTimeMillis();

        System.out.println("\n====== RESULT ======");
        System.out.println("Success: " + success.get());
        System.out.println("Failed : " + failed.get());
        System.out.println("Duration: " + (end - start) + " ms");
    }

    private static void connect() {

        try {

            Socket socket = new Socket(HOST, PORT);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            String key = generateKey();

            String request =
                    "GET / HTTP/1.1\r\n" +
                    "Host: " + HOST + ":" + PORT + "\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Key: " + key + "\r\n" +
                    "Sec-WebSocket-Version: 13\r\n\r\n";

            out.write(request.getBytes());
            out.flush();

            byte[] buffer = new byte[1024];
            int read = in.read(buffer);

            if (read > 0) {
                success.incrementAndGet();
            } else {
                failed.incrementAndGet();
            }

            // Keep connection open
            Thread.sleep(600000);

        } catch (Exception e) {
            failed.incrementAndGet();
        }
    }

    private static String generateKey() {

        byte[] nonce = new byte[16];
        new Random().nextBytes(nonce);

        return Base64.getEncoder().encodeToString(nonce);
    }
}