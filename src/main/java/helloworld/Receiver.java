package helloworld;

import com.rabbitmq.client.*;

public class Receiver {
  private final static String QUEUE_NAME = "hello";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    /*
     * Note that we declare the queue here, as well. Because we might start the consumer before the publisher,
     * we want to make sure the queue exists before we try to consume messages from it.
     * */
    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws java.io.IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
      }
    };

    channel.basicConsume(QUEUE_NAME, true, consumer);
  }
}
