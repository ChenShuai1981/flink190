package com.caselchen.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


public class GroovyCEPExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        kafkaConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("metric-event", new SimpleStringSchema(), kafkaConfig);
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        SingleOutputStreamOperator<MetricEvent> metricEvent = dataStreamSource
                .flatMap(new ParseMetricEventFunction()).returns(MetricEvent.class);

        Pattern p1 = ScriptEngine.getPattern(

                "  import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy\n" +
                        "import org.apache.flink.cep.pattern.Pattern\n" +
                        "import com.caselchen.flink.AviatorCondition \n" +

                        "where1 = new AviatorCondition(" +
                        "   \"getT(tags,\\\"cluster_name\\\")==\\\"terminus-x\\\"&&getF(fields,\\\"load5\\\")>15 \"" +
                        "        )\n" +

                        "def get(){ " +
                        "      return Pattern.begin(\"start\", AfterMatchSkipStrategy.noSkip())\n" +
                        "        .where(where1)" +
                        "}",
                "get");

        PatternStream pStream2 = CEP.pattern(
                metricEvent.keyBy(metricEvent1 -> metricEvent1.getName() +
                        Joiner.on(",").join(metricEvent1.getTags().values())
                ), p1);
        SingleOutputStreamOperator<String> filter2 = pStream2.select((PatternSelectFunction<MetricEvent, String>) pattern -> "-----------------------------" + pattern.toString());

        filter2.print();

        StateBackend memory = new MemoryStateBackend(10 * 5 * 1024 * 1024, true);
        env.setStateBackend(memory);
        env.execute("----flink cep alert ----");
    }
}
