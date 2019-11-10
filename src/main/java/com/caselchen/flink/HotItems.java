package com.caselchen.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
        env.setParallelism(1);
        env.enableCheckpointing(1000);

        // UserBehavior.csv 的本地文件路径
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
//        PojoTypeInfo<UserBehavior> typeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
//        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
//        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

//        Path filePath, TypeInformation[] fieldTypeInfos, String lineDelimiter, String fieldDelimiter, int[] selectedFields, boolean emptyColumnAsNull
        TypeInformation[] fieldTypeInfos = new TypeInformation[]{Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.LONG};
        String lineDelimiter = "\n";
        String fieldDelimiter = ",";
        int[] selectedFields = new int[]{0,1,2,3,4};
        boolean emptyColumnAsNull = true;
        RowCsvInputFormat csvInput = new RowCsvInputFormat(filePath, fieldTypeInfos, lineDelimiter, fieldDelimiter, selectedFields, emptyColumnAsNull);
//        RowCsvInputFormat csvInput = new RowCsvInputFormat(filePath, fieldTypeInfos);
        RowTypeInfo typeInfo = new RowTypeInfo(fieldTypeInfos);

        DataStream<Row> dataSource = env.createInput(csvInput, typeInfo);

//        DataStream<UserBehavior> dataSource = env.createInput(csvInput, typeInfo);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        dataSource.print();

//        DataStream<UserBehavior> timedData = dataSource
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
//                    @Override
//                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
//                        // 原始数据单位秒，将其转成毫秒
//                        return userBehavior.timestamp * 1000;
//                    }
//                });
//
//        DataStream<UserBehavior> pvData = timedData
//                .filter(new FilterFunction<UserBehavior>() {
//                    @Override
//                    public boolean filter(UserBehavior userBehavior) throws Exception {
//                        // 过滤出只有点击的数据
//                        return userBehavior.behavior.equals("pv");
//                    }
//                });
//
//        DataStream<ItemViewCount> windowedData = pvData
//                .keyBy("itemId")
//                .timeWindow(Time.minutes(60), Time.minutes(5))
//                .aggregate(new CountAgg(), new MyWindowFunction());
//
//        DataStream<String> topItems = windowedData
//                .keyBy("windowEnd")
//                .process(new TopNHotItems(3));  // 求点击量前3名的商品
//
//        topItems.print();
        env.execute("Hot Items Job");

    }

    /** 用户行为数据结构 **/
    public static class UserBehavior {
        public long userId;         // 用户ID
        public long itemId;         // 商品ID
        public int categoryId;      // 商品类目ID
        public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
        public long timestamp;      // 行为发生的时间戳，单位秒
    }

    /** COUNT 统计的聚合函数实现，每出现一条记录加一 */
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /** 用于输出窗口的结果 */
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /** 商品点击量(窗口操作的输出类型) */
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    /** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                ItemViewCount input,
                Context context,
                Collector<String> collector) throws Exception {

            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0;i<topSize;i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }
}
