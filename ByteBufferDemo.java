import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteBufferDemo {

    public static void main(String[] args) {

        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        System.out.println("Initial: position=" + buffer.position() +
                ", limit=" + buffer.limit());

        buffer.put("Hello".getBytes(StandardCharsets.UTF_8));

        System.out.println("After put: position=" + buffer.position() +
                ", limit=" + buffer.limit());

        buffer.flip();

        System.out.println("After flip: position=" + buffer.position() +
                ", limit=" + buffer.limit());

        while (buffer.hasRemaining()) {
            System.out.print((char) buffer.get());
        }

        System.out.println();

        buffer.clear();

        System.out.println("After clear: position=" + buffer.position() +
                ", limit=" + buffer.limit());
    }
}