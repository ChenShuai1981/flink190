package com.caselchen.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class MyDistinctFlatMapFunctionIntegrationTest {

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
        MyDistinctFlatMapFunctionIntegrationTest.CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements(121,122,123,122,131,121).keyBy(new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return 1;
            }
        })
                .flatMap(new MyDistinctFlatMapFunction())
                .addSink(new MyDistinctFlatMapFunctionIntegrationTest.CollectSink());

        // execute
        env.execute();

        // verify your results
        assertTrue(MyDistinctFlatMapFunctionIntegrationTest.CollectSink.values.containsAll(Arrays.asList(1L,2L,3L,3L,4L,4L)));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Long value, Context context) throws Exception {
            values.add(value);
        }
    }
}
