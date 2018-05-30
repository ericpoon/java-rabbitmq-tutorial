package topics;

import com.rabbitmq.client.*;

public class LogReceiver {
  private static final String EXCHANGE_NAME = "log_with_topics";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    // idempotent, just in case Emitter hasn't declare it
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
    String queueName = channel.queueDeclare().getQueue();

    if (args.length == 0) {
      System.err.println("Usage: LogReceiver [topic]");
      System.exit(1);
    }

    for (String topic : args) {
      channel.queueBind(queueName, EXCHANGE_NAME, topic); // use topic as binding key
      System.out.println(" [*] Will receive log with topic: " + topic);
    }

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
