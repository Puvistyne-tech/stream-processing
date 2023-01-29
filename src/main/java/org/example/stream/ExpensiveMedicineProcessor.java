package org.example.stream;


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.kafka.RecordGenerator;

import static org.example.stream.MyKafkaStreams.stream;

public class ExpensiveMedicineProcessor implements MyProcessorSupplier {
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

        if (record.value().getPrice() >= 4d) {
            System.out.println("**************************Processing record: " + record.value().price);
            context.forward(record);
        }

    }

    @Override
    public void close() {
    }

    public static void main(String[] args) {
        stream(
                "stream-2",
                (stream -> {
                    stream.process(ExpensiveMedicineProcessor::new).to("expensive");
                }),
                "secret"
        );
        System.out.println("expensive stream started");
    }
}
