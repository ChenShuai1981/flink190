package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TrackMaximumTemperature {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> sensorData = env
          // SensorSource generates random temperature readings
          .addSource(new SensorSource())
          // assign timestamps and watermarks which are required for event time
          .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)));

//        sensorData.print();

        DataStream<Tuple4<String, Long, Long, Double>> tenSecsMaxTemps = sensorData
          // project to sensor id and temperature
          .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
              @Override
              public Tuple2<String, Double> map(SensorReading value) throws Exception {
                  return new Tuple2(value.id, value.temperature);
              }
          })
          // compute every 10 seconds the max temperature per sensor
          .keyBy(new KeySelector<Tuple2<String,Double>, String>() {
              @Override
              public String getKey(Tuple2<String, Double> value) throws Exception {
                  return value.f0;
              }
          })
          .timeWindow(Time.seconds(10))
          .process(
                  new ProcessWindowFunction<Tuple2<String,Double>, Tuple4<String, Long, Long, Double>, String, TimeWindow>() {

                      private transient ValueState<Tuple4<String, Long, Long, Double>> maxTempState = null;

                      @Override
                      public void open(Configuration parameters) throws Exception {
                          ValueStateDescriptor<Tuple4<String, Long, Long, Double>> valueStateDesc = new ValueStateDescriptor("maxTempState", Types.TUPLE(Types.STRING, Types.LONG, Types.LONG, Types.DOUBLE));
                          // Method 2: 通过StateDescriptor的setQueryable方法使状态可查
                          valueStateDesc.setQueryable("maxTemperature");
                          maxTempState = getRuntimeContext().getState(valueStateDesc);
                      }

                      @Override
                      public void process(String key, Context context, Iterable<Tuple2<String, Double>> elements, Collector<Tuple4<String, Long, Long, Double>> out) throws Exception {
                          Double maxTemp = 0d;
                          for (Tuple2<String, Double> element : elements) {
                              if (element.f1 > maxTemp) {
                                  maxTemp = element.f1;
                              }
                          }

                          long winStart = context.window().getStart();
                          long winEnd = context.window().getEnd();
                          Tuple4<String, Long, Long, Double> maxElement = new Tuple4(key, winStart, winEnd, maxTemp);
                          maxTempState.update(maxElement);
                          out.collect(maxElement);
                      }
                  });

        tenSecsMaxTemps.print();

        // Method 1: 使用asQueryableState("xxx")通过Queryable State Stream使状态可查
        // store latest value for each sensor in a queryable state
//        tenSecsMaxTemps
//          .keyBy(new KeySelector<Tuple4<String,Long,Long,Double>, String>() {
//              @Override
//              public String getKey(Tuple4<String,Long,Long,Double> value) throws Exception {
//                  return value.f0;
//              }
//          })
//          .asQueryableState("maxTemperature");

        // execute application
        env.execute("Track max temperature");
    }
}
