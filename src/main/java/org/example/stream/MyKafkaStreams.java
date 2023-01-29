package org.example.stream;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.example.kafka.RecordGenerator;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.example.kafka.Utils.loadProperties;

public class MyKafkaStreams {

    public static void main(String[] args) {
//        stream(new SecretUserNameProcessor(),"medicine","secret");
//        System.out.println("secret stream started");
//
//
//        stream(new ExpensiveMedicineProcessor(),"secret","expensive");
//        System.out.println("expensive stream started");
    }

    public static void stream(String idConfig,Consumer<KStream<String, RecordGenerator>> processorConsumer, String inTopic) {
        Properties properties = loadProperties("streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, idConfig);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put("schema.registry.url", "http://localhost:8081");


        // Create a Serde configuration from the properties
        StreamsConfig config = new StreamsConfig(properties);

        // Create a Serde config for the generic type
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();

        keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
        valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

        // Create a Serde for the specific type
        SpecificAvroSerde<RecordGenerator> recordGeneratorSerde = new SpecificAvroSerde<>();
        recordGeneratorSerde.configure(serdeConfig, false);


        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, RecordGenerator> stream = builder.stream(inTopic,
                Consumed.with(stringSerde, recordGeneratorSerde));

//        KStream<String, RecordGenerator> anonymousKStream = stream.mapValues((k, v) -> {
////            System.out.println("Key: " + k);
////            System.out.println("Value: " + v);
//            v.setFirstName("***");
//            v.setLastName("***");
////            return KeyValue.pair(k, v);
//            return v;
//        }).filter((k, v) -> {
//            System.out.println((v.getPrice() > 4d) ? v.getPrice() : "---------" + v.getPrice());
//            return v.getPrice() > 4d;
//        });

//        stream.process(SecretUserNameProcessor::new).to("Secret_medicine");
//        stream.process(ExpensiveMedicineProcessor::new).to("expensive_medicine");
//        stream.process(()->{
//        }).to(out);
        processorConsumer.accept(stream);

//        anonymousKStream.to("Expensive_medicine");

        Topology topology = builder.build();


        final KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();


    }
}
