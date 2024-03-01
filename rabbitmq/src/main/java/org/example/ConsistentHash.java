package org.example;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsistentHash {
    public static void main(String args[]) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.64.39");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest123");
        Connection connection = factory.newConnection();
        Channel ch = connection.createChannel();
        ch.confirmSelect();
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        for(int i=0;i<100000;i++){
            ch.basicPublish("ex.hash", String.valueOf(i), builder.build(), "".getBytes("UTF-8"));
        }

        ch.waitForConfirmsOrDie(10000);
        System.out.println("Done publishing");
        connection.close();
    }
}
