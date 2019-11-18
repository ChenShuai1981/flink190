package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FavouriteColourTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 输入 name,color
//        DataStream<String> textStream = env.socketTextStream("localhost", 9999);

        DataStream<String> textStream = env.fromElements("Jack,red,1", "Rose,green,2", "Jack,yellow,3", "Bob,red,4", "Rose,yellow,5", "Bob,green,6");
        DataStream<NameColour> nameColourStream = textStream.map((MapFunction<String, NameColour>) value -> {
            String[] parts = value.split(",");
            return new NameColour(parts[0], parts[1], Long.valueOf(parts[2]));
        });

        StreamTableEnvironment stblEnv = StreamTableEnvironment.create(env);
        Table srcTbl = stblEnv.fromDataStream(nameColourStream, "name, colour, ts");
        stblEnv.registerTable("favourite_colour", srcTbl);

        Table resultTbl = stblEnv.sqlQuery("select t3.colour, count(*) as cnt from (\n" +
                "  select t1.* from favourite_colour t1 join (\n" +
                "    select name, max(ts) as ts from favourite_colour group by name\n" +
                "  ) t2 on t1.name = t2.name and t1.ts = t2.ts\n" +
                ") t3 group by colour");

        DataStream<Tuple2<Boolean, ColourCount>> resultStream = stblEnv.toRetractStream(resultTbl, ColourCount.class);

        // Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.
//        DataStream<ColourCount> resultStream = stblEnv.toAppendStream(tbl4, ColourCount.class);

        resultStream.print();

        // 输出 color,count
        env.execute("FavouriteColourTable");
    }

    public static class NameColour {
        public String name;
        public String colour;
        public long ts;

        public NameColour() {}

        public NameColour(String name, String colour, long ts) {
            this.name = name;
            this.colour = colour;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "NameColour{" +
                    "name=" + name +
                    ", colour='" + colour + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

    public static class ColourCount {
        public String colour;
        public long cnt;

        public ColourCount() {}

        public ColourCount(String colour, long cnt) {
            this.colour = colour;
            this.cnt = cnt;
        }

        @Override
        public String toString() {
            return "ColourCount{" +
                    "colour=" + colour +
                    ", cnt=" + cnt +
                    '}';
        }
    }

}
