package com.caselchen.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class QueryableStateByStateDescriptor {

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(parallelism);

        // 输入数据源：当前action产生时就上报一条数据，数据格式为 action,money
        SingleOutputStreamOperator<Tuple2<String, Long>> source = env.socketTextStream(host, port)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {

                        String[] splits = value.trim().split(",");

                        out.collect(new Tuple2<>(splits[0], Long.valueOf(splits[1])));
                    }
                });

        // 窗口统计: 计算最近5秒钟内，每个action的count(money)数、sum(money)数
        SingleOutputStreamOperator<Tuple5<String, String, String, Long, Long>> result = source
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.seconds(5))
                .process(new CustomCountSumWindowFunction());

        result.print();

        env.execute();
    }

    /** 自定义WindowFunction,统一每个Key对应的窗口的count数、sum数 */
    static class CustomCountSumWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Long>, Tuple5<String, String, String, Long, Long>, String, TimeWindow> {

        // 定义一个ValueState，来存放状态
        private transient ValueState<Tuple5<String, String, String, Long, Long>> sumCountValueState;

        /**
         * 算子CustomCountSumWindowFunction实例化时，只执行一次
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple5<String, String, String, Long, Long>> valueStateDescriptor =
                    new ValueStateDescriptor<>(
                            "lastFiveSecondsCountSumValueState",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG));

            // 通过ValueStateDescriptor.setQueryable 开放此状态,使此状态可查
            valueStateDescriptor.setQueryable("lastFiveSecondsCountSum");
            sumCountValueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        /**
         * 每个窗口都会调用一次process方法
         *
         * @param key 当前窗口对应的Key
         * @param context 窗口上下文
         * @param elements 当前窗口的所有元素
         * @param out 收集输出记录
         * @throws Exception
         */
        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple2<String, Long>> elements,
                Collector<Tuple5<String, String, String, Long, Long>> out)
                throws Exception {

            // 计算count数、sum数
            long currentCount = 0L;
            long currentSum = 0L;

            for (Tuple2<String, Long> value : elements) {
                currentCount += 1;
                currentSum += value.f1;
            }

            // 获取Window开始时间、结束时间
            TimeWindow window = context.window();
            String windowStartTime =
                    new DateTime(window.getStart(), DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss");
            String windowEndTime =
                    new DateTime(window.getEnd(), DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss");

            // 得到当前值
            Tuple5<String, String, String, Long, Long> currentValue =
                    new Tuple5<>(key, windowStartTime, windowEndTime, currentCount, currentSum);

            // 更新状态
            sumCountValueState.update(currentValue);

            // 输出结果
            out.collect(currentValue);
        }
    }
}

