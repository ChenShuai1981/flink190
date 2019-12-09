package com.caselchen.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.alibaba.fastjson.JSON;

import lombok.AllArgsConstructor;
import lombok.Data;

public class BroadcastStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MapStateDescriptor<Void, Pattern> bcStateDescriptor =
                new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer<String> patternConsumer = new FlinkKafkaConsumer("pattern", new SimpleStringSchema(), properties);
        patternConsumer.setStartFromLatest();
        FlinkKafkaConsumer<String> actionConsumer = new FlinkKafkaConsumer("action", new SimpleStringSchema(), properties);
        actionConsumer.setStartFromLatest();

        DataStream<Pattern> patterns = env.addSource(patternConsumer).map(new MapFunction<String, Pattern>() {
            @Override
            public Pattern map(String value) throws Exception {
                return JSON.parseObject(value, Pattern.class);
            }
        });

        BroadcastStream<Pattern> bcedPatterns = patterns.broadcast(bcStateDescriptor);

        DataStream<Action> actions = env.addSource(actionConsumer).map(new MapFunction<String, Action>() {
            @Override
            public Action map(String value) throws Exception {
                return JSON.parseObject(value, Action.class);
            }
        });

        KeyedStream<Action, Long> actionsByUser = actions
                .keyBy((KeySelector<Action, Long>) action -> action.userId);

        DataStream<Tuple2<Long, Pattern>> matches = actionsByUser
                .connect(bcedPatterns)
                .process(new PatternEvaluator());

        matches.print();

        env.execute("RuleBroadcastJob");
    }

    @Data
    @AllArgsConstructor
    public static class Action {
        private Long userId;
        private String action;
    }

    @Data
    @AllArgsConstructor
    public static class Pattern {
        private String firstAction;
        private String secondAction;
    }

    public static class PatternEvaluator
            extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Pattern>> {

        // handle for keyed state (per user)
        ValueState<String> prevActionState;
        // broadcast state descriptor
        MapStateDescriptor<Void, Pattern> patternDesc;

        @Override
        public void open(Configuration conf) {
            // initialize keyed state
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAction", Types.STRING));
            patternDesc = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        }

        /**
         * Called for each user action.
         * Evaluates the current pattern against the previous and
         * current action of the user.
         */
        @Override
        public void processElement(
                Action action,
                ReadOnlyContext ctx,
                Collector<Tuple2<Long, Pattern>> out) throws Exception {
            // get current pattern from broadcast state
            Pattern pattern = ctx
                    .getBroadcastState(this.patternDesc)
                    // access MapState with null as VOID default value
                    .get(null);
            // get previous action of current user from keyed state
            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // user had an action before, check if pattern matches
                if (pattern.firstAction.equals(prevAction) &&
                        pattern.secondAction.equals(action.action)) {
                    // MATCH
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // update keyed state and remember action for next pattern evaluation
            prevActionState.update(action.action);
        }

        /**
         * Called for each new pattern.
         * Overwrites the current pattern with the new pattern.
         */
        @Override
        public void processBroadcastElement(
                Pattern pattern,
                Context ctx,
                Collector<Tuple2<Long, Pattern>> out) throws Exception {
            // store the new pattern by updating the broadcast state
            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(patternDesc);
            // storing in MapState with null as VOID default value
            bcState.put(null, pattern);
        }
    }

}
