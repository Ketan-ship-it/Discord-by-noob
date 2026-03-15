import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class BlockingEchoServer {

    private static final int PORT = 9000;

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Blocking Echo Server started on port " + PORT);

        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Accepted connection from " + client.getRemoteSocketAddress());
            System.out.println("Thread Count = " + Thread.activeCount());
            new Thread(() -> handleClient(client)).start();
        }
    }

    private static void handleClient(Socket client) {
        try (
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(client.getInputStream()));
            PrintWriter out = new PrintWriter(client.getOutputStream(), true)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                out.println("Echo: " + line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}