package com.modules;

import static com.modules.Constants.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/*
    The second part of the module. The Message Consumer connects to the
    server and consumes incomming messages to queues. The exchange must
    be selected by the user. The routing key determines which queue to
    consume.
*/
public class MessageConsumer {
    
    // Creating a channel and a DeliverCallback as a logger so that when
    // messages come in, the callback is fired and prints the message.
    private Channel messagingChannel;
    private DeliverCallback callback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        System.out.println(message);
    };
    
    public MessageConsumer() {}
    
    public void initialize() {
        // Initializing the connection factory and setting hostname.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOSTNAME);
        
        try {
            
            // Initializing a connection and getting a channel out of it.
            Connection connection = factory.newConnection();
            this.messagingChannel = connection.createChannel();
            this.connectToExchange();
            this.createQueue();
            this.bindQueueToExchange();
        } catch ( TimeoutException | IOException e ) {
            System.err.println("Error occured during connection of channel.");
        }
    }
    
    public void initialize(
        String hostname,
        String exchangeName,
        String exchangeType,
        String queue,
        String routingKey
    ) {
        // Initializing the connection factory and setting hostname.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostname);
        
        try {
            
            // Initializing a connection and getting a channel out of it.
            Connection connection = factory.newConnection();
            this.messagingChannel = connection.createChannel();
            this.connectToExchange(exchangeName, exchangeType);
            this.createQueue(queue);
            this.bindQueueToExchange(queue, exchangeName, routingKey);
        } catch ( TimeoutException | IOException e ) {
            System.err.println("Error occured during connection of channel.");
        }
    }
    
    // Declaring an exchange or connecting to it if it already exists,
    // based on the default setting in the 'Constants' file.
    private void connectToExchange() {
        try {
            this.messagingChannel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        } catch ( IOException e ) {}
    }
    
    // Declaring an exchange or connecting to it if it already exists,
    // based on the criteria given by the user.
    private void connectToExchange(String exchangeName, String exchangeType) {
        try {
            this.messagingChannel.exchangeDeclare(exchangeName, exchangeType);
        } catch ( IOException e ) {}
    }
    
    // Creating a queue based on the default setting
    private void createQueue() {
        try {
            this.messagingChannel.queueDeclare(QUEUE, false, false, false, null);
        } catch ( IOException e ) {}
    }
    
    // Creating a queue based on the given criteria
    private void createQueue(String queueName) {
        try {
            this.messagingChannel.queueDeclare(queueName, false, false, false, null);
        } catch ( IOException e ) {}
    }
    
    // Binding the queue to the exchange and selecting the routing key for
    // which queue messages should be consumed by the consumer.
    private void bindQueueToExchange() {
        try {
            this.messagingChannel.queueBind(QUEUE, EXCHANGE_NAME, ROUTING_KEY);
        } catch ( IOException e ) {}
    }
    
    // Binding the queue to the exchange and selecting the routing key
    // based on the parameters passed by the user
    private void bindQueueToExchange(String queue, String exchangeName, String routingKey) {
        try {
            this.messagingChannel.queueBind(queue, exchangeName, routingKey);
        } catch ( IOException e ) {}
    }
    
    // Closing the channel to prevent any memory leaks.
    public void closeChannel() {
        try {
            this.messagingChannel.close();
        } catch ( TimeoutException | IOException e ) {}
    }
    
    // Starting to consume the queue messages. The important required arguments
    // are the queue name and the callback to determine the process on the message.
    public void startConsuming() {
        try {
            this
                .messagingChannel
                .basicConsume(QUEUE, true, callback, consumerTag -> {});
        } catch ( IOException e ) {}
    }
    
    // Starting to consume the queue messages. The two important arguments are
    // passed by the user.
    public void startConsuming(String queue, DeliverCallback callback) {
        try {
            this
                .messagingChannel
                .basicConsume(QUEUE, true, callback, consumerTag -> {});
        } catch ( IOException e ) {}
    }
            
}



