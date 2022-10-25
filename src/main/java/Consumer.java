import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Consumer {

    private final static String QUEUE_NAME = "skiersQueue";
    private final static String RABBITMQ_URL = "localhost";
    private final static Integer N_CONSUMER_THREAD = 1000;
    private static ConcurrentHashMap<String,String> map;


    public static void main(String[] argv) throws Exception {
        map = new ConcurrentHashMap<>();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_URL);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(N_CONSUMER_THREAD);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            executor.execute(new DataDumper(map,message));
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }
}