package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class ConfigSender {
    public static void main(String[] args) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig, new StringSerializer(), new StringSerializer());

        CustomerPurchaseBehaviorTracker.Config config = new CustomerPurchaseBehaviorTracker.Config(
                CustomerPurchaseBehaviorTracker.Channel.APP, new Date(), 1, 1);

        String message = JSON.toJSONString(config);
        System.out.println(message);

        producer.send(new ProducerRecord<>("app_config", null, message));
        producer.close();
    }
}
