package work_queues;

import com.rabbitmq.client.*;

public class Worker {
  private final static String TASK_QUEUE_NAME = "task_queue";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null); // durable
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    channel.basicQos(1);

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws java.io.IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
        try {
          doWork(message);
        } finally {
          System.out.println(" [x] Done");
          channel.basicAck(envelope.getDeliveryTag(), false); // send basic ack
        }
      }
    };

    channel.basicConsume(TASK_QUEUE_NAME, false, consumer); // manual ack is on
  }

  private static void doWork(String task) {
    for (char c : task.toCharArray()) {
      if (c == '.') {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException _ignored) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

}
