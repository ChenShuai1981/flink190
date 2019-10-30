package com.caselchen.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        List<Tuple2<String, Double>> curFTemp = IntStream.rangeClosed(0, 9)
                .boxed()
                .map(i -> new Tuple2<String, Double>("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20)))
                .collect(Collectors.toList());

        while (running) {
            curFTemp.forEach(t -> {
                String id = t.f0;
                Double temperature = t.f1 + rand.nextGaussian() * 0.5;
                SensorReading sr = new SensorReading(id, System.currentTimeMillis(), temperature);
//                System.out.println(sr);
                ctx.collect(sr);
            });
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
