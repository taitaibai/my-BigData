package com.lgt;

import com.lgt.component.WordCountMapper;
import com.lgt.component.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordCountCombinerApp {
    private static final String HDFS_URL = "hdfs://192.168.32.28:8020";
    private static final String HADOOP_USER_NAME = "root";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Input and output paths are necessary");
            return;
        }

        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);

        Configuration conf = new Configuration();
        //指明HDFS地址
        conf.set("fs.defaultFS",HDFS_URL);

        try {
            //创建一个Job

            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCountApp.class);

            //设置Mapper和Reducer
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            //设置Mapper 输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置Reducer输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //设置 Combiner
            job.setCombinerClass(WordCountReducer.class);

            //如果输出目录已纯在，则删除，否则会抛出异常
            FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), conf, HADOOP_USER_NAME);
            Path outputPath = new Path(args[1]);
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            //设置作业输入文件和输出文件路径
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job,outputPath);

            //将作业提交到集群并等待完成，参数设置true打印显示对应的进度
            boolean resultStatus = job.waitForCompletion(true);

            fileSystem.close();

            System.exit(resultStatus ? 0 : -1);
        } catch (IOException | URISyntaxException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
