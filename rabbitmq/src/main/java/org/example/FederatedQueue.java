package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FederatedQueue {
    static class SampleConsumer extends Thread{

        private final String mQueueName;

        public SampleConsumer (String queueName) {
            this.mQueueName = queueName;
        }
        public void run() {
            try{
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("192.168.64.42");
                factory.setPort(5672);
                factory.setUsername("guest");
                factory.setPassword("guest");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                channel.queueDeclare(mQueueName, /*durable*/true, /*exclusive*/false, /*autodelete*/false, /*arguments*/null);
                System.out.println(" [x] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Received message " + message);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);


                };

                channel.basicConsume(mQueueName, false, deliverCallback, consumerTag ->{ });
                sleep(Long.MAX_VALUE);

            }
            catch(IOException | InterruptedException | TimeoutException e){
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws InterruptedException {
        SampleConsumer consumer = new SampleConsumer("q.test");
        consumer.start();

        consumer.join();

        System.out.println("Done");
    }
}
