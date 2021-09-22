package com.lgt;

import com.lgt.utils.WordCountDataUtils;

public class App {

    public static void main(String[] args) {
        WordCountDataUtils.generateDataToHDFS("hdfs://192.168.32.28:8020","root","/wordcount/input.txt");
    }
}
