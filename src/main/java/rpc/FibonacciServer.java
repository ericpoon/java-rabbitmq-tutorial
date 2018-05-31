package rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FibonacciServer {
  private static final String RPC_QUEUE_NAME = "rpc_queue_name";

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
            .Builder()
            .correlationId(properties.getCorrelationId())
            .build();
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
        String[] args = message.split(" ");
        int n = Integer.parseInt(args[1]);
        if (args[0].equals("fib")) {
          System.out.println(" [*] Calculating 'fib(" + n + ")'");
          int fibResult = fib(n);
          System.out.println(" [x] Sent '" + fibResult + "'");
          channel.basicPublish("", properties.getReplyTo(), replyProps, Integer.toString(fibResult).getBytes("UTF-8"));
          channel.basicAck(envelope.getDeliveryTag(), false); // send basic ack
        }
      }
    };

    channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
    channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

  }

  private static int fib(int n) {
    if (n == 0) return 0;
    if (n == 1) return 1;
    return fib(n - 1) + fib(n - 2);
  }

}
