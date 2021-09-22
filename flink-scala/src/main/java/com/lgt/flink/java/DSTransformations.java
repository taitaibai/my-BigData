package com.lgt.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DSTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * map
         */
        /*DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        integerDataStreamSource.map((MapFunction<Integer, Object >) value -> value * 2).print();*/

        /**
         * flatmap
         */

        /*String string01 = "one one one two two";
        String string02 = "third third third";
        DataStreamSource<String> stringDataStreamSource = env.fromElements(string01, string02);
        stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String s1 : s.split(" ")) {
                    collector.collect(s1);
                }
            }
        }).print();*/

        /**
         * filter
         */

        //env.fromElements(1, 2, 3, 4, 5).filter(x -> x > 3).print();

        /**
         * KeyBy and Reduce
         */

        /*DataStreamSource<Tuple2<String, Integer>> tuple2DS = env.fromElements(new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 3),
                new Tuple2<>("b", 5));*/

        /*KeyedStream<Tuple2<String, Integer>, Tuple> keyStream = tuple2DS.keyBy(0);
        keyStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (v1, v2) ->
                new Tuple2<>(v1.f0, v1.f1 + v2.f1)).print();*/
        /*tuple2DS.keyBy(0).sum(1).print();*/

        /**
         * Union
         */

        /*DataStreamSource<Tuple2<String, Integer>> streamSource01 = env.fromElements(new Tuple2<>("a", 1),
                new Tuple2<>("a", 2));

        DataStreamSource<Tuple2<String, Integer>> streamSource02 = env.fromElements(new Tuple2<>("b", 1),
                new Tuple2<>("b", 2));

        streamSource01.union(streamSource02).print();
        streamSource01.union(streamSource01, streamSource02);*/

        /*DataStreamSource<Tuple2<String, Integer>> streamSource01 = env.fromElements(new Tuple2<>("a", 3),
                new Tuple2<>("b", 5));

        DataStreamSource<Integer> streamSource02 = env.fromElements(2, 3, 9);

        ConnectedStreams<Tuple2<String, Integer>, Integer> connect = streamSource01.connect(streamSource02);
        connect.map(new CoMapFunction<Tuple2<String, Integer>, Integer, Integer>() {
            @Override
            public Integer map1(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }

            @Override
            public Integer map2(Integer value) throws Exception {
                return value;
            }
        }).map(x -> x * 100).print();*/

        /*DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        SplitStream<Integer> split = streamSource.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<>();
                output.add(value % 2 == 0 ? "even" : "odd");
                return output;
            }
        });

        split.select("even").print();*/

        /*DataStreamSource<Tuple3<String, Integer, String>> streamSource = env.fromElements(new Tuple3<>("li", 22, "2018-09-23"),
                new Tuple3<>("ming", 33, "2020-09-23"));
        streamSource.project(0, 2).print();*/

        DataStreamSource<Tuple2<String, Integer>> streamSource = env.fromElements(new Tuple2<>("Hadoop", 1),
                new Tuple2<>("Spark", 1),
                new Tuple2<>("Flink-streaming", 2),
                new Tuple2<>("Flink-batch", 4),
                new Tuple2<>("Hbase", 3));

        streamSource.partitionCustom(new Partitioner<String>() {

            @Override
            public int partition(String key, int numPartition) {
                return key.toLowerCase().contains("flink") ? 0 : 1;
            }
        }, 0).print();



        env.execute("DSTransformations ");
    }
}
