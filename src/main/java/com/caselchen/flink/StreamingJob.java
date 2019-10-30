/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.caselchen.flink;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KafkaConstantProperties.KAFKA_BROKER);
        kafkaProps.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(
                KafkaConstantProperties.FLINK_COMPUTE_TOPIC_IN1,
                new SimpleStringSchema(),
                kafkaProps);

        DataStream<String> dataStream = env.addSource(myConsumer);
        dataStream.map(new MapFunction<String, AppBehavior>() {
            @Override
            public AppBehavior map(String value) throws Exception {
                return null;
            }
        });




        // 四元组数据为: 订单号,统计维度标识,订单数,订单金额
        DataStream<Tuple4<String, String, Integer, Double>> counts = dataStream
                .flatMap(new TestBizDataLineSplitter())
                .keyBy(1)
                .timeWindow(Time.of(30, TimeUnit.SECONDS))
                .reduce((value1, value2) -> {
                    return new Tuple4<>(value1.f0, value1.f1, value1.f2 + value2.f2, value1.f3 + value2.f3);
                });

        // 暂时输入与输出相同
        counts.addSink(new FlinkKafkaProducer<>(
                KafkaConstantProperties.FLINK_DATA_SINK_TOPIC_OUT1,
                new KafkaTuple4StringSchema(),
                kafkaProps)
        );
        // 统计值多向输出
        dataStream.print();
        counts.print();
        env.execute("Test Count from Kafka data");
	}

}
