package org.example;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class PriorityQueue {
    private static final String QUEUE_NAME = "q.test-priority";
    private static final int MESSAGE_COUNT = 300_000;

    private static final int MAX_PRIORITY = 255;

    public static void produceMessages(int cnt) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.64.39");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest123");

        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            Map<String, Object>args = new HashMap<String, Object>();
            args.put("x-max-priority",MAX_PRIORITY);

            channel.queueDeclare(QUEUE_NAME, /*durable*/true, /*exclusive*/false, /*autodelete*/false, /*arguments*/null);
            channel.queuePurge(QUEUE_NAME);

            ByteBuffer buffer = ByteBuffer.allocate(1000);

            for(int i=0;i<MESSAGE_COUNT; i++){
                try {
                    AMQP.BasicProperties props = new AMQP.BasicProperties
                            .Builder()
                            .priority(i%MAX_PRIORITY)
                            .deliveryMode(2)
                            .build();

                    channel.basicPublish("", QUEUE_NAME, props, buffer.array());
                }
                catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
        catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }

    }

    public static void main(String args[]) {
        long start = System.nanoTime();
        produceMessages(MESSAGE_COUNT);
        long end = System.nanoTime();
        System.out.format("Published %d messages in %d ms %n", MESSAGE_COUNT, Duration.ofNanos(end-start).toMillis());
    }
}
