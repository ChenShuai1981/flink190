package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class FavouriteColour {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 输入 name,color
//        DataStream<String> textStream = env.socketTextStream("localhost", 9999);

        DataStream<String> textStream = env.fromElements("Jack,red", "Rose,green", "Jack,yellow", "Bob,red", "Rose,yellow", "Bob,green");
        DataStream<NameColour> nameColourStream = textStream.map(new MapFunction<String, NameColour>() {
            @Override
            public NameColour map(String value) throws Exception {
                String[] parts = value.split(",");
                return new NameColour(parts[0], parts[1]);
            }
        });


        DataStream<Collection<NameColour>> keyByNameStream = nameColourStream
                .process(new ProcessFunction<NameColour, Collection<NameColour>>() {
                    private Map<String, String> nameColourMap = new HashMap<>();
                    @Override
                    public void processElement(NameColour value, Context ctx, Collector<Collection<NameColour>> out) throws Exception {
                        List<NameColour> nameColours = new ArrayList<>();
                        nameColourMap.put(value.getName(), value.getColour());
                        for (Map.Entry<String, String> entry : nameColourMap.entrySet()) {
                            NameColour nc = new NameColour(entry.getKey(), entry.getValue());
                            nameColours.add(nc);
                        }
                        out.collect(nameColours);
                    }
                });

        keyByNameStream.process(new ProcessFunction<Collection<NameColour>, ColourCount>() {

            @Override
            public void processElement(Collection<NameColour> nameColours, Context ctx, Collector<ColourCount> out) throws Exception {
                System.out.println();
                System.out.println("===============");
                System.out.println();
                Map<String, Set<String>> map = new HashMap<>();
                for (NameColour nameColour : nameColours) {
                    String name = nameColour.getName();
                    String colour = nameColour.getColour();
                    if (!map.containsKey(colour)) {
                        map.put(colour, new HashSet<>());
                    }
                    map.get(colour).add(name);
                }

                for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                    String colour = entry.getKey();
                    int count = entry.getValue().size();
                    out.collect(new ColourCount(colour, Long.valueOf(count)));
                }
            }
        }).print();

        // 输出 color,count
        env.execute("FavouriteColour");
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class NameColour {
        private String name;
        private String colour;
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class ColourCount {
        private String colour;
        private Long count;
    }

}
