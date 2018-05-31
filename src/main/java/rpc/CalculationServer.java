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
        try {
          int n = Integer.parseInt(args[1]);
          int result = calculate(args[0], n);
          channel.basicPublish("", properties.getReplyTo(), replyProps, String.valueOf(result).getBytes("UTF-8"));
          System.out.println(" [x] Sent '" + result + "'");
        } catch (NumberFormatException e) {
          String errMessage = "'" + args[1] + "' is not a valid integer";
          channel.basicPublish("", properties.getReplyTo(), replyProps, errMessage.getBytes("UTF-8"));
          System.out.println(" [x] Error message sent back to client");
        } catch (RuntimeException e) {
          channel.basicPublish("", properties.getReplyTo(), replyProps, e.getMessage().getBytes("UTF-8"));
          System.out.println(" [x] Error message sent back to client");
        } finally {
          channel.basicAck(envelope.getDeliveryTag(), false); // send basic ack
        }

      }
    };

    channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
    channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

  }

  private static int calculate(String procedure, int n) {
    switch (procedure) {
      case "fib":
        return fib(n);
      case "fact":
        return fact(n);
      default:
        throw new RuntimeException("[unknown procedure] Please use the following:\n\t- fibonacci (fib)\n\t- factorial (fact)\n");
    }
  }

  private static int fib(int n) {
    if (n < 0) throw new RuntimeException("[invalid input] Fibonacci requires non-negative integer input");
    if (n == 0) return 0;
    if (n == 1) return 1;
    return fib(n - 1) + fib(n - 2);
  }

  private static int fact(int n) {
    if (n < 0) throw new RuntimeException("[invalid input] Factorial requires non-negative integer input");
    if (n == 0) return 1;
    return fact(n - 1) * n;
  }

}
