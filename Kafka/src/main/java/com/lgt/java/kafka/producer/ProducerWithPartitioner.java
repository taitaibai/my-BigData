package com.lgt.java.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerWithPartitioner {

    public static void main(String[] args) {
        String topicName = "Kafka-Partitoner-Test";
        Properties props = ProducerWithPartitioner.getProperties();
        props.put("partitioner.class", "com.lgt.java.kafka.partitioner.CustomPartitioner");
        props.put("pass.line", 6);

        Producer<Integer, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <= 10; i++) {
            String score = "score:" + i;
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, i, score);
            /**
             * 异步发送消息
             */
            producer.send(record, ((recordMetadata, e) ->
                    System.out.printf("%s,partition=%d,\n", score, recordMetadata.partition())));
        }
        producer.close();
    }

    private static Properties getProperties() {
        Properties props=new Properties();
        props.put("bootstrap.servers", "hadoop1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
