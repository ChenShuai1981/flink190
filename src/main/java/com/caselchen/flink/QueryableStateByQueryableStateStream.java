package com.caselchen.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class QueryableStateByQueryableStateStream {

    public static void main(String[] args) throws Exception {

        // 设置参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String host = parameterTool.get("host", "localhost");
        final int port = parameterTool.getInt("port", 1234);
        final int parallelism = parameterTool.getInt("parallelism", 4);

        // 配置环境
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(parallelism);
        env.enableCheckpointing(1000);

        // 输入数据源：每行数据格式 event,pv
        SingleOutputStreamOperator<Tuple2<String, Long>> source =
                env.socketTextStream(host, port)
                        .flatMap(
                                new FlatMapFunction<String, Tuple2<String, Long>>() {
                                    @Override
                                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {

                                        String[] splits = value.trim().split(",");

                                        out.collect(new Tuple2<>(splits[0], Long.valueOf(splits[1])));
                                    }
                                });

        // 窗口统计: 最近5秒钟内，每个事件的最大pv
        SingleOutputStreamOperator<Tuple2<String, Long>> result =
                source
                        .keyBy(
                                new KeySelector<Tuple2<String, Long>, String>() {
                                    @Override
                                    public String getKey(Tuple2<String, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                })
                        .timeWindow(Time.seconds(5))
                        .max(1);

        // 输出结果
        result.print();

        // 使得结果的状态可查
        // asQueryableState 返回QueryableStateStream
        // QueryableStateStream类似于一个接收器，无法进行进一步转换
        // QueryableStateStream接收传入的数据并更新状态
        result
                .keyBy(
                        new KeySelector<Tuple2<String, Long>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Long> value) throws Exception {
                                return value.f0;
                            }
                        })
                .asQueryableState("lastFiveSecondsMaxPV");

        env.execute();
    }
}

