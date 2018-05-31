package rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class CalculationServer {
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
        System.out.println(" [*] Calculating '" + args[0] + "(" + n + ")'");

        String result = calculate(args[0], n);
        channel.basicPublish("", properties.getReplyTo(), replyProps, result.getBytes("UTF-8"));
        channel.basicAck(envelope.getDeliveryTag(), false); // send basic ack
        System.out.println(" [x] Sent '" + result + "'");

      }
    };

    channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
    channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

  }

  private static String calculate(String procedure, int n) {
    int result;
    switch (procedure) {
      case "fib":
        result = fib(n);
        break;
      case "fact":
        result = fact(n);
        break;
      default:
        return "[unknown procedure] We only support fibonacci('fib'), factorial('fact')";
    }
    return String.valueOf(result);
  }

  private static int fib(int n) {
    if (n == 0) return 0;
    if (n == 1) return 1;
    return fib(n - 1) + fib(n - 2);
  }

  private static int fact(int n) {
    if (n == 0) return 1;
    return fact(n - 1) * n;
  }

}
