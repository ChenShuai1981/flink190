package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerTestCoutinue {
    public static void main(String[] args) {
        Producer();
    }

    public static void Producer() {
        String broker = "localhost:9092";
        String topic = "zzz";
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        String[] depLists = new String[5];
        depLists[0] = "行政部";
        depLists[1] = "账务部";
        depLists[2] = "市场部";
        depLists[3] = "技术部";
        depLists[4] = "销售部";

        Random rand = new Random(300);
        for (int i = 1; i <= 10000; i++) {
            String temp = JSON.toJSONString(
                    new Employee(i, "user" + i, "password" + i, rand.nextInt(40) + 20, (rand.nextInt(300) + 1) * 100, depLists[rand.nextInt(5)])
            );
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, temp);
            producer.send(record);
            System.out.println("发送数据: " + temp);
            try {
                Thread.sleep(500); //发送一条数据 sleep
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("发送数据完成");
        producer.flush();
    }

}