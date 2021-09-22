package com.lgt.utils;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * @author lgt
 */
public class WordCountDataUtils {

    public static final List<String> WORD_LIST = Arrays.asList("Spark", "Hadoop", "Hbase", "Hive", "Flink");
    private static Configuration configuration = new Configuration();
    /**
     * 模拟产生词频数据
     *
     * @return 词频数据
     */
    private static String generateData() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            Collections.shuffle(WORD_LIST);
            Random random = new Random();
            int endIndex = random.nextInt(WORD_LIST.size()) % (WORD_LIST.size()) + 1;
            String line = StringUtils.join(WORD_LIST.toArray(), "\t", 0, endIndex);
            builder.append(line).append("\n");
        }
        return builder.toString();
    }

    /**
     * 模拟产生词频数据并输出到本地
     *
     * @param outputPath
     */
    public static void generateDataToLocal(String outputPath) {
        Path path = Paths.get(outputPath);
        try {
            if (Files.exists(path)) {
                Files.delete(path);
            }
            Files.write(path, generateData().getBytes(), StandardOpenOption.CREATE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void generateDataToHDFS(String hdfsUrl, String user, String outputPathString) {
        FileSystem fileSystem = null;
        try {
            //configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            fileSystem = FileSystem.get(new URI(hdfsUrl), configuration, user);
            org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputPathString);
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FSDataOutputStream out = fileSystem.create(outputPath);
            out.write(generateData().getBytes());
            out.flush();
            out.close();
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
