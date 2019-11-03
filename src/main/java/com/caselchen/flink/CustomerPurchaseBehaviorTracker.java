package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.java.Log;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * http://ju.outofmemory.cn/entry/371335
 * 用户购物路径长度跟踪场景描述
 *
 * 针对用户在手机App上操作行为的事件，通过跟踪用户操作来实时触发指定的操作。
 * 假设我们关注一个用户在App上经过多次操作之后，比如浏览了几个商品、
 * 将浏览过的商品加入购物车、将购物车中的商品移除购物车等等，最后发生了购买行为，
 * 那么对于用户从开始到最终达成购买所进行操作的行为的次数，
 * 我们定义为用户购物路径长度，通过这个概念假设可以通过推送优惠折扣权限、
 * 或者适时地提醒用户使用App等运营活动，能够提高用户的复购率，这个是我们要达成的目标。
 *
 * 一个用户在App上操作行为我们定义有如下几种：
 *
 * VIEW_PRODUCT
 * ADD_TO_CART
 * REMOVE_FROM_CART
 * PURCHASE
 *
 * 用户行为数据
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:27:11","data":{"productId":196}}
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12_09:43:18","data":{"productId":126}}
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12_09:27:11","data":{"productId":126}}
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12_09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
 *
 * 配置数据
 * {"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}
 *
 * 生成统计数据
 * {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","purchasePathLength":9,"eventTypeCounts":{"ADD_TO_CART":1,"PURCHASE":1,"VIEW_PRODUCT":7}}
 *
 */
@Log
public class CustomerPurchaseBehaviorTracker {

    public static void main(String[] args) throws Exception {
        log.info("Input args: " + Arrays.asList(args));
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: CustomerPurchaseBehaviorTracker " +
                    "--input-event-topic <topic> " +
                    "--input-config-topic <topic> " +
                    "--output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--group.id <some id>");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new FsStateBackend(
//                "hdfs://localhost:9000/flink-checkpoints/customer-purchase-behavior-tracker"));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(60 * 1000);
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create customer user event stream
        final FlinkKafkaConsumer kafkaUserEventSource = new FlinkKafkaConsumer<>(
                parameterTool.getRequired("input-event-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        // (userEvent, userId)
        final KeyedStream<UserEvent, String> customerUserEventStream = env
                .addSource(kafkaUserEventSource)
                .map((MapFunction<String, UserEvent>) s -> UserEvent.buildEvent(s))
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.hours(1)))
                .keyBy((KeySelector<UserEvent, String>) userEvent -> userEvent.getUserId());

        // create dynamic configuration event stream
        final FlinkKafkaConsumer kafkaConfigEventSource = new FlinkKafkaConsumer<>(
                parameterTool.getRequired("input-config-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        final MapStateDescriptor<String, Config> configStateDescriptor =
                new MapStateDescriptor<>(
                        "configBroadcastState",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<Config>() {}));

        final BroadcastStream<Config> configBroadcastStream = env
                .addSource(kafkaConfigEventSource)
                .map((MapFunction<String, Config>) value -> Config.buildConfig(value))
                .broadcast(configStateDescriptor);

        final FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer<>(
                parameterTool.getRequired("output-topic"),
                new EvaluatedResultSchema(),
                parameterTool.getProperties());

        // connect above 2 streams
        DataStream<EvaluatedResult> connectedStream = customerUserEventStream
                .connect(configBroadcastStream)
                .process(new ConnectedBroadcastProcessFuntion())
                .setParallelism(1);
//        connectedStream.addSink(kafkaProducer);
        connectedStream.print();

        env.execute("UserPurchaseBehaviorTracker");
    }

    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

        public CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(UserEvent element) {
            return element.getEventTime();
        }

    }

    @Data
    public static class UserEvent implements Serializable {
        private String userId;
        private Channel channel;
        private EventType eventType;
        private long eventTime;
        private PurchaseData data;

        public static UserEvent buildEvent(String s) {
            return JSON.parseObject(s, UserEvent.class);
        }
    }

    @Data
    @AllArgsConstructor
    public static class Config implements Serializable {
        private Channel channel;
        private Date registerDate;
        private int historyPurchaseTimes;
        private int maxPurchasePathLength;

        public static Config buildConfig(String s) {
            return JSON.parseObject(s, Config.class);
        }
    }

    @Data
    public static class EvaluatedResult {
        private String userId;
        private Channel channel;
        private int purchasePathLength;
        private Map<EventType, Integer> eventTypeCounts;
    }

    public enum EventType {
        VIEW_PRODUCT, ADD_TO_CART, REMOVE_FROM_CART, PURCHASE
    }

    public enum Channel {
        WEB, APP
    }

    @Data
    public static class PurchaseData {
        private long productId;
        private float price;
        private float amount;
    }

    public static class EvaluatedResultSchema implements SerializationSchema<EvaluatedResult> {

        @Override
        public byte[] serialize(EvaluatedResult element) {
            return new byte[0];
        }
    }

    @Data
    public static class UserEventContainer {
        private String userId;
        private List<UserEvent> userEvents;

    }

    public static class ConnectedBroadcastProcessFuntion extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {

        private MapStateDescriptor<Channel, Config> configStateDescriptor = new MapStateDescriptor("configBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Config>() {}));
        private MapStateDescriptor<Channel, Map<String, UserEventContainer>> userMapStateDesc = new MapStateDescriptor("userEventContainerState", BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(String.class, UserEventContainer.class));
        private Config defaultConfig = new Config(Channel.APP, new Date(), 1, 1);

        @Override
        public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult> out) throws Exception {
            String userId = value.getUserId();
            Channel channel = value.getChannel();

            EventType eventType = value.getEventType();
            Config config = ctx.getBroadcastState(configStateDescriptor).get(channel);
            log.info("Read config: channel=" + channel + ", config=" + config);
            if (Objects.isNull(config)) {
                config = defaultConfig;
            }

            final MapState<Channel, Map<String, UserEventContainer>> state =
                    getRuntimeContext().getMapState(userMapStateDesc);

            // collect per-user events to the user map state
            Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
            if (Objects.isNull(userEventContainerMap)) {
                userEventContainerMap = Maps.newHashMap();
                state.put(channel, userEventContainerMap);
            }
            if (!userEventContainerMap.containsKey(userId)) {
                UserEventContainer container = new UserEventContainer();
                container.setUserId(userId);
                userEventContainerMap.put(userId, container);
            }
            userEventContainerMap.get(userId).getUserEvents().add(value);

            // check whether a user purchase event arrives
            // if true, then compute the purchase path length, and prepare to trigger predefined actions
            if (eventType == EventType.PURCHASE) {
                log.info("Receive a purchase event: " + value);
                Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
                result.ifPresent(r -> out.collect(result.get()));
                // clear evaluated user's events
                state.get(channel).remove(userId);
            }
        }

        @Override
        public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {
            Channel channel = value.getChannel();
            BroadcastState<Channel, Config> state = ctx.getBroadcastState(configStateDescriptor);
            final Config oldConfig = ctx.getBroadcastState(configStateDescriptor).get(channel);
            if(state.contains(channel)) {
                log.info("Configured channel exists: channel=" + channel);
                log.info("Config detail: oldConfig=" + oldConfig + ", newConfig=" + value);
            } else {
                log.info("Config detail: defaultConfig=" + defaultConfig + ", newConfig=" + value);
            }
            // update config value for configKey
            state.put(channel, value);
        }

        private Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
            Optional<EvaluatedResult> result = Optional.empty();
            Channel channel = config.getChannel();
            int historyPurchaseTimes = config.getHistoryPurchaseTimes();
            int maxPurchasePathLength = config.getMaxPurchasePathLength();

            int purchasePathLen = container.getUserEvents().size();
            if (historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
                // sort by event time
                container.getUserEvents().sort(Comparator.comparingLong(UserEvent::getEventTime));

                final Map<EventType, Integer> stat = Maps.newHashMap();
                container.getUserEvents().stream()
                        .collect(Collectors.groupingBy(UserEvent::getEventType))
                        .forEach((eventType, events) -> stat.put(eventType, events.size()));

                final EvaluatedResult evaluatedResult = new EvaluatedResult();
                evaluatedResult.setUserId(container.getUserId());
                evaluatedResult.setChannel(channel);
                evaluatedResult.setEventTypeCounts(stat);
                evaluatedResult.setPurchasePathLength(purchasePathLen);
                log.info("Evaluated result: " + JSON.toJSONString(evaluatedResult));
                result = Optional.of(evaluatedResult);
            }
            return result;
        }
    }
}
