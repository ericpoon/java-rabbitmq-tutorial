package publish_subscribe_fanout;

import com.rabbitmq.client.*;

public class LogReceiver {
  private static final String EXCHANGE_NAME = "logs";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    // idempotent, just in case Emitter hasn't declare it
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
    String queueName = channel.queueDeclare().getQueue();
    channel.queueBind(queueName, EXCHANGE_NAME, "");

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws java.io.IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
      }
    };

    channel.basicConsume(queueName, true, consumer);
  }
}
