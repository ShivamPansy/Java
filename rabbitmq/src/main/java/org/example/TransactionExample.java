package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TransactionExample {
    private static final String QUEUE_NAME = "q.transaction.example";

    static class SampleProducer extends Thread{
        public void run() {
            System.out.println("---> Running Producer...");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("192.168.64.39");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest123");
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel())
            {
                channel.queueDeclare(QUEUE_NAME, /*durable*/false , /*exclusive*/false, /*autodelete*/false, /*argument*/null);
                channel.queuePurge(QUEUE_NAME);

                //channel.txSelect();

                for(int i=0;i<=5;i++){
                    String message = "Hello World " + i;
                    if(i==5){
                        message = "Final message " + i;
                    }
                    channel.basicPublish(/*exchange*/"", /*routingkey*/QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent " + message );
                    sleep(2000);
                }

                //channel.txCommit();

            } catch (IOException | TimeoutException | InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    static class SampleConsumer extends Thread{
        public void run() {
            try{
                sleep(1000);
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("192.168.64.39");
                factory.setPort(5672);
                factory.setUsername("guest");
                factory.setPassword("guest123");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autodelete*/false, /*arguments*/null);
                System.out.println(" [x] Waiting for messages....");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    channel.txSelect();
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Received message " + message);
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                    if(message.startsWith("Final message")){
                        channel.txRollback();
                        channel.basicCancel("");
                    }
                };

                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag ->{ });
                sleep(Long.MAX_VALUE);

            }
            catch(IOException | InterruptedException | TimeoutException e){
                e.printStackTrace();
            }
        }

    }
    public static void main(String args[]) throws InterruptedException {
        SampleProducer producer = new SampleProducer();
        producer.start();

        SampleConsumer consumer = new SampleConsumer();
        consumer.start();

        producer.join();
        consumer.join();

        System.out.println("Done");

    }
}
