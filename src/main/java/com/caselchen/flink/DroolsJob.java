package com.caselchen.flink;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class DroolsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Person> inputStream = env.fromElements(
                new Person("p1",8),
                new Person("p2",10),
                new Person("p3",18),
                new Person("p4",15),
                new Person("p5",25));

        DroolsAsyncFunc asyncFunc = new DroolsAsyncFunc("src/main/resources/rules/sample.drl");
        DataStream<String> outputStream = AsyncDataStream.unorderedWait(inputStream, asyncFunc,
                6000, TimeUnit.MILLISECONDS, 100);
        outputStream.print();

        env.execute("DroolsJob");
    }
}
