package org.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.example.client.Producer;

import java.io.InputStream;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Utils {

    /**
     * this method generates a random json message
     *
     * @return a random json message
     */
    public static String generateMessageJSON(Object message) {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return json;
    }

    /**
     * Create a topic
     *
     * @param topic       topic to create
     * @param nbPartition number of partitions
     * @param nbReplica  number of replicas
     * @return NewTopic object
     */
    public static NewTopic createTopic(String topic, int nbPartition, int nbReplica) {
        Properties cloudConfig = loadProperties("config");

        NewTopic newTopic = new NewTopic(topic,
                nbPartition,
                (short) nbReplica);
//                Optional.of(Integer.parseInt(cloudConfig.getProperty("num.partitions"))),
//                Optional.of(Short.parseShort(cloudConfig.getProperty("replication.factor"))));
        try (
                AdminClient adminClient = AdminClient.create(cloudConfig);
        ) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
        return newTopic;
    }

    /**
     * Load the properties from the properties file
     *
     * @param propsName name of the properties file
     * @return Properties object
     */
    public static Properties loadProperties(String propsName) {
        final Properties cloudConfig = new Properties();
        try (
                var input = Producer.class.getClassLoader().getResourceAsStream(propsName + ".properties")
        ) {
            cloudConfig.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return cloudConfig;
    }



    /**
     * Load the schema from the resources folder
     *
     * @param schemaName
     * @return Schema
     */
    public static Schema loadSchema(String schemaName) {
        try (
                InputStream input = Utils.class.getClassLoader().getResourceAsStream(schemaName + ".avsc")
        ) {
            return new Schema.Parser().parse(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



}
