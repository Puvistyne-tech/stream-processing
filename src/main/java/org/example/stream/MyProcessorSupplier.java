package org.example.stream;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.example.kafka.RecordGenerator;

public interface MyProcessorSupplier extends Processor<String, RecordGenerator, String, RecordGenerator> {
}
