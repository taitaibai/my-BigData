package com.lgt.java.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    public static void main(String[] args) {
        String topicName = "Hello-Kafka";
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /**
         * 创建生产者
         */
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <10 ; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello" + i, "world" + i);

            /**
             * 发送消息
             */

            /*RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(record).get();
                System.out.printf("topic=%s,partition=%d,offset=%s \n",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }*/

            /**
             * 异步发送消息，并监听回调
             */
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("进行异常处理");
                    } else {
                        System.out.printf("topic=%s,partition=%d,offset=%s \n",
                                recordMetadata.topic(),recordMetadata.partition(),recordMetadata.offset());
                    }
                }
            });
        }
        producer.close();
    }
}
