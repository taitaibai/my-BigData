package com.lgt.java.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        String topic = "Hello-Kafka";
        String group = "group1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop1:9092");
        props.put("group.id", group);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));

        /*try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s,partition = %d,key = %s,value = %s,offset = %d, \n",
                            record.topic(), record.partition(), record.key(), record.value(), record.offset());
                }
            }
        } finally {
            consumer.close();
        }*/

        /**
         * 同步提交
         */

        /*try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                }

                consumer.commitSync();
            }
        }finally {
            consumer.close();
        }*/

        /**
         * 异步提交
         */
        /*try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                }

                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if (e != null) {
                            System.out.println("错误处理");
                            map.forEach((x, y) -> System.out.printf("topic=%s,partition=%d,offset=%s \n",
                                    x.topic(), x.partition(), y.offset()));
                        }
                    }
                });
            }
        } finally {
            consumer.close();
        }*/

        /*
         * 同步加异步提交
         */
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                }
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                consumer.commitSync();
            }finally {
                consumer.close();
            }
        }

    }

}
