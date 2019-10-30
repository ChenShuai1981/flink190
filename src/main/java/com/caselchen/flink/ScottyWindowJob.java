package com.caselchen.flink;

import de.tub.dima.scotty.core.windowType.SessionWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import de.tub.dima.scotty.flinkconnector.demo.windowFunctions.SumWindowFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class ScottyWindowJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Instantiate Scotty window operator
        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> windowOperator =
                new KeyedScottyWindowOperator<>(new SumWindowFunction());

        // Add multiple windows to the same operator
        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 1000));
        windowOperator.addWindow(new SlidingWindow(WindowMeasure.Time, 1000, 5000));
        windowOperator.addWindow(new SessionWindow(WindowMeasure.Time, 1000));

        DataStream<Tuple2<Integer, Integer>> stream = env.addSource(new DemoSource());
        // Add operator to Flink job
        stream.keyBy(0)
                .process(windowOperator).print();

        env.execute("ScottyWindowJob");
    }
}
