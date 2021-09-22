package com.lgt.flink.java;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iotdb.flink.DefaultIoTSerializationSchema;
import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.IoTSerializationSchema;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FlinkToIoTDBSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IoTDBSinkOptions options = new IoTDBSinkOptions();
        options.setHost("192.168.32.28");
        options.setPort(6667);
        options.setUser("root");
        options.setPassword("root");
        options.setStorageGroup("root.flink");

        options.setTimeseriesOptionList(
                Lists.newArrayList(
                        new IoTDBSinkOptions.TimeseriesOption(
                                "root.flink.d1.s1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY
                        )
                )
        );

        IoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
        IoTDBSink ioTDBSink = new IoTDBSink(options, serializationSchema).withBatchSize(10).withSessionPoolSize(3);
        env.addSource(new SourceFunction<Map<String, String>>() {
            boolean running = true;
            Random random = new SecureRandom();
            @Override
            public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
                while (running) {
                    Map<String, String> tuple=new HashMap();
                    tuple.put("device", "root.flink.d1");
                    tuple.put("timestamp", String.valueOf(System.currentTimeMillis()));
                    tuple.put("measurements", "s1");
                    tuple.put("types", "DOUBLE");
                    tuple.put("values", String.valueOf(random.nextDouble()));

                    sourceContext.collect(tuple);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).name("sensor-source")
                .setParallelism(1)
                .addSink(ioTDBSink)
                .name("iotdb-sink");

        env.execute("iotdb-flink");
    }

    private static class SensorSource implements SourceFunction<Map<String, String>>{
        boolean running = true;
        Random random = new SecureRandom();
        @Override
        public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
            while (running) {
                Map<String, String> tuple=new HashMap();
                tuple.put("device", "root.sg.d1");
                tuple.put("timestamp", String.valueOf(System.currentTimeMillis()));
                tuple.put("measurements", "s1");
                tuple.put("types", "DOUBLE");
                tuple.put("values", String.valueOf(random.nextDouble()));

                sourceContext.collect(tuple);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
