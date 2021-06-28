package com.leyantech.sh.kafka;

import com.leyantech.sh.jooq.tables.pojos.Shop;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author ：thyxsl
 * @date ：Created in 2021/6/28 18:50
 * @version: 1.0
 */
public class ShopConsumer {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test", "bar"));
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, byte[]> record : records) {
                ByteArrayInputStream in = new ByteArrayInputStream(record.value());
                ObjectInputStream sIn = new ObjectInputStream(in);
                Shop object = (Shop)sIn.readObject();
                System.out.println(object);
            }
        }
    }
}
