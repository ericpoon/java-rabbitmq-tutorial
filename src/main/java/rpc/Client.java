package rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Client {
  private static final String RPC_QUEUE_NAME = "rpc_queue_name";

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    final String corrId = UUID.randomUUID().toString();
    final String replyQueueName = channel.queueDeclare().getQueue();
    AMQP.BasicProperties props = new AMQP.BasicProperties
        .Builder()
        .correlationId(corrId)
        .replyTo(replyQueueName)
        .build();

    if (args.length < 2) {
      System.err.println("Usage: Client [procedure-name] [number]");
      System.exit(1);
    }

    String message = args[0] + " " + args[1];

    channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
    channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));
    System.out.println(" [x] Sent '" + message + "'");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        if (properties.getCorrelationId().equals(corrId)) {
          String message = new String(body, "UTF-8");
          try {
            int result = Integer.parseInt(message);
            System.out.println(" [x] Received calculation result: " + result);
          } catch (NumberFormatException e) {
            System.err.println(" [x] Error occurred:\n     " + message);
          } finally {
            connection.close();
          }
        }
      }
    };

    channel.basicConsume(replyQueueName, consumer);
  }
}
