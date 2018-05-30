package direct_routing;

import com.rabbitmq.client.*;

public class LogReceiver {
  private static final String EXCHANGE_NAME = "multi_type_logs";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    // idempotent, just in case Emitter hasn't declare it
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    String queueName = channel.queueDeclare().getQueue();

    if (args.length == 0) {
      System.err.println("Usage: LogReceiver [info] [warning] [error]");
      System.exit(1);
    }

    for (String severity : args) {
      channel.queueBind(queueName, EXCHANGE_NAME, severity); // use severity as binding key
      System.out.println(" [*] Will receive log with severity level: " + severity);
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
