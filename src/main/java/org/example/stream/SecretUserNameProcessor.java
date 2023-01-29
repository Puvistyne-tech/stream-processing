package org.example.stream;


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.kafka.RecordGenerator;

import static org.example.stream.MyKafkaStreams.stream;

public class SecretUserNameProcessor implements MyProcessorSupplier {
    private ProcessorContext<String, RecordGenerator> context;

    @Override
    public void init(ProcessorContext<String, RecordGenerator> processorContext) {
        this.context = processorContext;
    }

    /**
     * Convert firstname and lastname to '***' and send to output topic
     *
     * @param record the record to process
     */
    @Override
    public void process(Record<String, RecordGenerator> record) {

        record.value().setFirstName("***");
        record.value().setLastName("***");

        System.out.println("**************************Processing record: " + record);
        context.forward(record);

    }

    @Override
    public void close() {
    }

    public static void main(String[] args) {
        stream(
                "stream-1",
                (stream) -> {
                    stream.process(SecretUserNameProcessor::new).to("secret");
                },
                "medicine"
        );
        System.out.println("secret stream started");
    }
}
