package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 子订单消息
 * {
 *     "userId": 234567,
 *     "orderId": 2902306918400,
 *     "subOrderId": 2902306918401,
 *     "siteId": 10219,
 *     "siteName": "site_blabla",
 *     "cityId": 101,
 *     "cityName": "北京市",
 *     "warehouseId": 636,
 *     "merchandiseId": 187699,
 *     "price": 299,
 *     "quantity": 2,
 *     "orderStatus": 1,
 *     "isNewOrder": 0,
 *     "timestamp": 1572963672217
 * }
 *
 * 获取各个站点site的GMV统计信息：
 * HGETALL RT:DASHBOARD:GMV:2019-11-19:SITES
 *
 * 获取商品TopN统计信息：
 * ZREVRANGEBYSCORE RT:DASHBOARD:RANKING:2019-11-19:MERCHANDISE +inf -inf WITHSCORES
 */
public class DashboardJob {
    public static void main(String[] args) throws Exception {
        String ORDER_EXT_TOPIC_NAME = "order";
        int PARTITION_COUNT = 1;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dashboard");

        DataStream<String> sourceStream = env
                .addSource(new FlinkKafkaConsumer<>(
                        ORDER_EXT_TOPIC_NAME,                        // topic
                        new SimpleStringSchema(),                    // deserializer
                        consumerProps                                // consumer properties
                ))
                .setParallelism(PARTITION_COUNT)
                .name("source_kafka_" + ORDER_EXT_TOPIC_NAME)
                .uid("source_kafka_" + ORDER_EXT_TOPIC_NAME);

        DataStream<SubOrderDetail> orderStream = sourceStream
                .map(message -> JSON.parseObject(message, SubOrderDetail.class))
                .name("map_sub_order_detail").uid("map_sub_order_detail");

        // 按站点统计，开一天窗口，每秒刷新统计信息
        WindowedStream<SubOrderDetail, Tuple, TimeWindow> siteDayWindowStream = orderStream
                .keyBy("siteId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

        // 统计站点的订单数、子订单数、商品件数以及成交金额
        DataStream<OrderAccumulator> siteAggStream = siteDayWindowStream
                .aggregate(new OrderAndGmvAggregateFunc())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        // <站点id,统计json>
        DataStream<Tuple2<Long, String>> siteResultStream = siteAggStream
                .keyBy((KeySelector<OrderAccumulator, Long>) value -> value.getSiteId())
                .process(new OutputOrderGmvProcessFunc(), TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}))
                .name("process_site_gmv_changed").uid("process_site_gmv_changed");

        // 看官请自己构造合适的FlinkJedisPoolConfig
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        // 站点统计指标写入redis
        siteResultStream
                .addSink(new RedisSink<>(jedisPoolConfig, new GmvRedisMapper()))
                .name("sink_redis_site_gmv").uid("sink_redis_site_gmv")
                .setParallelism(1);

        // 按商品维度统计
        WindowedStream<SubOrderDetail, Tuple, TimeWindow> merchandiseWindowStream = orderStream
                .keyBy("merchandiseId")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)));

        DataStream<Tuple2<Long, Long>> merchandiseRankStream = merchandiseWindowStream
                .aggregate(new MerchandiseSalesAggregateFunc(), new MerchandiseSalesWindowFunc())
                .name("aggregate_merch_sales").uid("aggregate_merch_sales")
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() { }));

        // 商品Top N写入 redis
        merchandiseRankStream
                .addSink(new RedisSink<>(jedisPoolConfig, new RankingRedisMapper()))
                .name("sink_redis_merchandise_rank").uid("sink_redis_merchandise_rank")
                .setParallelism(1);

        env.execute("DashboardJob");
    }
}
