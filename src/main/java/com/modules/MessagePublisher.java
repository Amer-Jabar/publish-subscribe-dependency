package com.modules;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import static com.modules.Constants.*;

/*
    This is a module that helps in minizing the code written for implementing
    a publisher and subscriber in an architecture that requires a message broker.
    The metadata about the message broker is up to the user to select. In my case,
    I am using RabbitMQ that already runs on port 15672.

    The project consists of two modules: the publiser, and the subscriber.

    Notes:
        - The whole of this module depends on the RabbitMQ library and is not from scratch.
        - I created this module because the current Spring AMQP version's message
            listener does not work with the RabbitMQ and the two parties have not
            yet solved the solution.
        - From the sources where I have borrowed codes, it is claimed that the code
            is not production ready and requires alot more of advancement.
        - I have stored some constant variables in the 'Constant' file that the
            user can change. The variables are for more reusability.
*/
public class MessagePublisher {
    
    private Channel messagingChannel;
    
    public MessagePublisher() {}
    
    public void initialize() {
        // Initializing connection factory and setting the default hostname.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOSTNAME);
        
        try {
            
            // Initializing a connection and getting a channel out of it.
            Connection connection = factory.newConnection();
            this.messagingChannel = connection.createChannel();
            this.createExchange();
        } catch ( IOException | TimeoutException e ) {
            System.err.println(e);
        }
    }
     
    // Creating an exchange in the RabbitMQ server based on the given name
    // and type in the 'Constants' file.
    public void createExchange() {
        try {
            this.messagingChannel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        } catch ( IOException e ) {}
    }

    // Creating an exchange in the RabbitMQ server based on the parameters
    // given by the user.
    public void createExchange(String exchangeName, String exchangeType) {
        try {
            this.messagingChannel.exchangeDeclare(exchangeName, exchangeType);
        } catch ( IOException e ) {}
    }
    
    // Sending messages to the exchange and selecting the default routing key
    // in order to forward the message to a queue. The payload must be in bytes.
    public void sendMessage(String str) {
        try {
            this
                .messagingChannel
                .basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, str.getBytes());
        } catch ( IOException e ) {}
    }
    
    // Sending messages to the exchange and passing a specific routing key
    // in order to forward the message to a queue. The payload must be in bytes.
    public void sendMessage(String str, String routingKey) {
        try {
            this
                .messagingChannel
                .basicPublish(EXCHANGE_NAME, routingKey, null, str.getBytes());
        } catch ( IOException e ) {}
    }
    
    // Closing the channel to prevent any type of memory leaks.
    public void closeChannel() {
        try {
            this.messagingChannel.close();
        } catch ( TimeoutException | IOException e ) {}
    }
    
}
