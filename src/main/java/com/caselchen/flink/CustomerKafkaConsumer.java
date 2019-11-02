package com.caselchen.flink;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class CustomerKafkaConsumer<T> extends FlinkKafkaConsumer<T> {

    private AbsDeserialization<T> valueDeserializer;

    public CustomerKafkaConsumer(String topic, AbsDeserialization<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        valueDeserializer.setRuntimeContext(getRuntimeContext());
        valueDeserializer.initMetric();
        super.run(sourceContext);
    }

}