package com.demo;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Sender {
  private final static String QUEUE_NAME = "hello";

  public static void main(String[] args)
      throws java.io.IOException, java.util.concurrent.TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

  }
}
