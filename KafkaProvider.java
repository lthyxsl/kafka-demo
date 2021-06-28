package com.leyantech.sh;

import com.leyantech.sh.jooq.tables.pojos.Shop;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;

/**
 * @author ：thyxsl
 * @date ：Created in 2021/6/28 21:04
 * @version: 1.0
 */
public class KafkaProvider {
    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream write = new ObjectOutputStream(out);
            Shop shop = new Shop(i + r.nextInt(), i, i, LocalDateTime.now(), LocalDateTime.now());
            write.writeObject(shop);
            System.out.println("发送消息 " + i);
            producer.send(new ProducerRecord<>("test", i + "", out.toByteArray()));
            write.flush();
            out.flush();
        }

        producer.close();


    }
}
