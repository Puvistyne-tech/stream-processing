package org.example.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.example.kafka.Message;
import org.example.kafka.RecordGenerator;

import java.util.Properties;
import java.util.function.BiConsumer;

import static org.example.client.Consumer.showMedicines;
import static org.example.kafka.RecordGenerator.generateAvroRecord;
import static org.example.kafka.Utils.*;

public class Producer {

    public static void main(String[] args) {



        //produceAvro("try", 1, 1);

        var topic = "pharmacy";

        //Quesiton 1
//        produceString(topic);

        //Question 2
        produceAvro(
                topic,
                3,
                3,
                (schema, producer) -> {
                    for (int i = 0; i < 500; i++) {

                        // create avro record
                        var avroRecord = generateAvroRecord(schema);

                        // Create producer record
                        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
                                topic,
//                    Integer.parseInt(avroRecord.get("pharmacyId").toString()) % nbPartition,
//                    avroRecord.get("cip").toString(),
                                avroRecord
                        );

                        // Send record
                        producer.send(record);
                        System.out.println("<---- Sent: " + record);

                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            System.out.println(e.getMessage());
                        }
                    }
                }
        );

    }

    /**
     * This is a kafka string producer
     *
     * @param topic the topic to produce to
     *              these produces to 3 partitions of the topic pharmacy based on the pharmacyId
     */
    public static void produceString(String topic) {
        Properties properties = loadProperties("producer");

        try (
                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ) {
            for (int i = 0; i < 100; i++) {
                var message = new Message();
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic,
                        String.valueOf(message.cip),
                        generateMessageJSON(message)
                );
                producer.send(record);
                System.out.println("<---- Sent: " + record);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This is a kafka avro producer
     *
     * @param topic the topic to produce to
     *              this produces to 3 partitions of the topic pharmacy based on the pharmacyId
     */
    public static void produceAvro(String topic, int nbPartition, int nbReplica, BiConsumer<Schema, KafkaProducer<String, GenericRecord>> javaConsumer) {

        //load producer properties
        Properties properties = loadProperties("producer.avro");

        // Create Kafka producer properties
        Schema schema = loadSchema("record");

        // Create Kafka producer
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        // Create topics
        createTopic(topic, nbPartition, nbReplica);

        // produce
        javaConsumer.accept(schema, producer);


        // close producer
        producer.flush();
        producer.close();

    }


}
