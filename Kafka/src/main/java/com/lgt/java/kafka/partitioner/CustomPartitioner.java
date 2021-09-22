package com.lgt.java.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private int passLine;
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /**
         * key 值为分数，当分数大于分数线时，分配到1分区，否则分配到0分区
         */
        return (Integer) key >= passLine ? 1 : 0;
    }

    @Override
    public void close() {
        System.out.println("分区器关闭");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        /**
         * 从生产者配置中获取分数线
         */
        passLine = (Integer) configs.get("pass.line");
    }
}
