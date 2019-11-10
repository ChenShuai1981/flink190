package com.caselchen.flink;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.OutputTag;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class AlarmFunctionIntegrationTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testIncrementPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        AlarmFunctionIntegrationTest.CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        OutputTag<Integer> alarmTag = new OutputTag<Integer>("alarm"){};
        SingleOutputStreamOperator<Integer> inputStream = env.fromElements(15,16,17,18,19,20)
                .process(new AlarmFunction());
        DataStream<Integer> alarmStream = inputStream.getSideOutput(alarmTag);

        inputStream.addSink(new AlarmFunctionIntegrationTest.CollectSink());
        alarmStream.addSink(new AlarmFunctionIntegrationTest.CollectAlarmSink());

        // execute
        env.execute();

        // to ensure pipeline run completely
        Thread.sleep(2000);

        // verify your results
        System.out.println(AlarmFunctionIntegrationTest.CollectSink.values);
        assertTrue(AlarmFunctionIntegrationTest.CollectSink.values.containsAll(Arrays.asList(15,16,17,18,19,20)));

        System.out.println(AlarmFunctionIntegrationTest.CollectAlarmSink.values);
        assertTrue(AlarmFunctionIntegrationTest.CollectAlarmSink.values.containsAll(Arrays.asList(15,16,17)));
    }

    // create a testing output sink
    private static class CollectSink implements SinkFunction<Integer> {

        // must be static
        public static final List<Integer> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Integer value, Context context) throws Exception {
            values.add(value);
        }
    }

    // create a testing alarm sink
    private static class CollectAlarmSink implements SinkFunction<Integer> {

        // must be static
        public static final List<Integer> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Integer value, Context context) throws Exception {
            values.add(value);
        }
    }
}
