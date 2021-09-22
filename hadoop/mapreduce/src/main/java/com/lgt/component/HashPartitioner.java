package com.lgt.component;

import com.lgt.WordCountApp;
import com.lgt.utils.WordCountDataUtils;
import org.apache.hadoop.mapreduce.Partitioner;

public class HashPartitioner<K,V> extends Partitioner<K,V> {

    @Override
    public int getPartition(K k, V v, int numReducerTasks) {
        return (k.hashCode() & Integer.MAX_VALUE) % numReducerTasks;
    }
}
