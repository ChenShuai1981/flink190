package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.caselchen.flink.CustomerPurchaseBehaviorTracker.*;

import java.util.Properties;
import java.util.UUID;

public class UserEventSender {
    public static void main(String[] args) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig, new StringSerializer(), new StringSerializer());
        UserEvent userEvent = new UserEvent();
        userEvent.setUserId(UUID.randomUUID().toString());
        userEvent.setChannel(Channel.APP);
        userEvent.setEventTime(System.currentTimeMillis());
        userEvent.setEventType(EventType.VIEW_PRODUCT);
        String message = JSON.toJSONString(userEvent);
        System.out.println(message);
        producer.send(new ProducerRecord<>("user_events", userEvent.getUserId(), message));
        producer.close();
    }
}
