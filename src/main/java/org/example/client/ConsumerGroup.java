package org.example.client;

import org.example.kafka.Utils;

import static org.example.client.Consumer.multiConsumer;

public class ConsumerGroup {
    public static void main(String[] args) {
        multiConsumer("medicine", 3);
    }
}
