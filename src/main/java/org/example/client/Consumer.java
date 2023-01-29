package org.example.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.example.kafka.RecordGenerator;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.example.kafka.Utils.*;

public class Consumer {

    public static HashMap<Integer, Double> medicines = new HashMap<Integer, Double>();

    private static void addMedicine(int cip, double price) {
//        if (medicines.containsKey(cip))
//            medicines.put(cip, medicines.get(cip) + price);
//        else
//            medicines.put(cip, price);
        medicines.computeIfPresent(cip, (k, v) -> v + price);
        medicines.putIfAbsent(cip, price);
    }

    public static void showMedicines() {
        medicines.forEach((k, v) -> System.out.println("CIP: " + k + " - Price: " + v));
        System.out.println("Total Number of Medicines: " + medicines.size());
    }

    public static void main(String[] args) {

//        “streamée”
        var topic = "pharmacy";
        var newTopic = "medicine";

        //Question 1
        //consumeString(topic);

        //Question 2
        createTopic(newTopic, 3, 3);

        Consumer.consumeAvro(
                topic,
                (genericRecord, producer) -> {
                    ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
                            newTopic,
                            genericRecord.get("cip").toString(),
                            genericRecord
                    );
                    producer.send(record);
                    System.out.println("<---- Sent: " + record);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                    }
//                    producer.close();

                }
        );


    }

    /**
     * This is a kafka string consumer
     *
     * @param topic the topic to consume from
     */
    public static void consumeString(String topic) {

        Properties properties = loadProperties("consumer");
        properties.put("group.id", "admin");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));

        boolean running = true;
        while (running) {
            System.out.println("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("---> Received: " + record.value());
//                System.out.println(record.toString());
            }
        }

        consumer.close();
    }

    /**
     * This is a kafka avro consumer
     *
     * @param topic the topic to consume from
     */
    public static void consumeAvro(String topic, java.util.function.BiConsumer<RecordGenerator, KafkaProducer<String, GenericRecord>> javaConsumer) {

        //load consumer properties
        Properties props = loadProperties("consumer.avro");

        //load avro schema
        Schema schema = loadSchema("record");


        try (
                KafkaConsumer<String, RecordGenerator> consumer = new KafkaConsumer<>(props);
        ) {
            //subscribe to topic
            consumer.subscribe(Collections.singletonList(topic));

            //poll for messages
            consumer.partitionsFor("pharmacy").forEach(System.out::println);
            consumer.partitionsFor("medicine").forEach(System.out::println);
            consumer.assignment().forEach(System.out::println);


            AdminClient client = AdminClient.create(props);
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false); // include internal topics
            ListTopicsResult topics = client.listTopics(options);
            Set<String> topicNames = topics.names().get().stream().filter(s -> s.equals(topic)).collect(Collectors.toSet());
            DescribeTopicsResult result = client.describeTopics(topicNames);
            Map<String, TopicDescription> topicDescriptionMap = result.all().get();
            for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
                String topicName = entry.getKey();
                TopicDescription topicDescription = entry.getValue();
                List<TopicPartitionInfo> partitions = topicDescription.partitions();
                for (TopicPartitionInfo partition : partitions) {
                    int partitionId = partition.partition();
                    List<Node> replicas = partition.replicas();
                    System.out.println("Topic: " + topicName + ", partition: " + partitionId + ", replicas: " + replicas);
                }
            }

            KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(loadProperties("producer.avro"));


            var isRunning = true;
            while (isRunning) {
                System.out.println(" _ ");
                ConsumerRecords<String, RecordGenerator> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, RecordGenerator> record : records) {
                    RecordGenerator avroRecord = record.value();
                    System.out.println();
                    System.out.print("----> " + record.partition() + " :::: ");
                    System.out.println(avroRecord);
                    if (record != null) {
                        javaConsumer.accept(avroRecord, producer);
                        System.out.println();
                    }
                }
            }
            consumer.close();
            producer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create multiple consumers and subscribe them to the same topic
     *
     * @param topic          topic to subscribe to
     * @param numOfConsumers number of consumers to create
     *                       Note: The number of consumers should be less than or equal to the number of partitions
     */
    public static void multiConsumer(String topic, int numOfConsumers) {
        // Configuration for the consumers
        var props = loadProperties("consumer.avro");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");

        // Create the consumers
        KafkaConsumer[] consumers = new KafkaConsumer[numOfConsumers];
        for (int i = 0; i < numOfConsumers; i++) {
            consumers[i] = new KafkaConsumer<>(props);
        }

        // Subscribe the consumers to the topic
        for (var consumer : consumers) {
            consumer.subscribe(Collections.singletonList(topic));
        }

        var running = true;
        // Start polling for messages
        while (running) {
            System.out.println(" _ ");
            for (KafkaConsumer<String, RecordGenerator> consumer : consumers) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (Iterator<ConsumerRecord<String, RecordGenerator>> iterator = records.iterator(); iterator.hasNext(); ) {
                    ConsumerRecord<String, RecordGenerator> record = iterator.next();
                    // Process the record
                    System.out.println();
                    System.out.println("----> " + record);
                    // il faut faire un hashmap pour
                    Consumer.addMedicine(record.value().getCip(), record.value().getPrice());
                    //how to print a custome message only at the end of the loop

                    if (!iterator.hasNext()) {
                        showMedicines();
                        System.out.println();
                    }
                }
            }
        }

        // Close the consumers
        for (var consumer : consumers) {
            consumer.close();
        }


    }

}




