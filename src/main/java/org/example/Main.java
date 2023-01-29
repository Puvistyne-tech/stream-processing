package org.example;

import java.io.IOException;

import static org.example.client.Producer.produceString;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Producing Pharmacy!");

        var topic = "Newpharmacy";

//        multiConsumer(topic, 3);

        startStringProducer();
    }

    private static void startStringProducer() {
        var topic = "pharmacy";

        produceString(topic);

    }
}