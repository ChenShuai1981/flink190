package com.caselchen.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StatefulProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource()).assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)));

        KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy((KeySelector<SensorReading, String>) value -> value.id);

        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData.process(new SelfCleaningTemperatureAlertFunction(1.5));

        alerts.print();

        env.execute("Generate Temperature Alerts");
    }
}
