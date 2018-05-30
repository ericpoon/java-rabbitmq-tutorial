package direct_routing;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class LogEmitter {
  private static final String EXCHANGE_NAME = "multi_type_logs";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

    String message = getMessage(args);
    String severity = getSeverity(args);

    channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8")); // use severity as direct_routing key
    System.out.println(" [x] Sent '" + message + "'");

    channel.close();
    connection.close();
  }

  private static String getMessage(String[] strings) {
    if (strings.length <= 1) {
      return "[empty message]";
    }
    return joinStrings(strings, " ", 1);
  }

  private static String joinStrings(String[] strings, String delimiter, int startIndex) {
    int length = strings.length;
    if (length == 0) return "";
    StringBuilder words = new StringBuilder(strings[startIndex]);
    for (int i = startIndex + 1; i < length; i++) {
      words.append(delimiter).append(strings[i]);
    }
    return words.toString();
  }

  private static String getSeverity(String[] strings) {
    if (strings.length < 1) return "info";
    return strings[0];
  }

}
