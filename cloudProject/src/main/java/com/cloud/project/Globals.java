package com.cloud.project;

/**
 * Class which behaves as a configuration file for global attributes
 */
public class Globals {

    public static String getNamenodeUrl() {
        return "hdfs://localhost:9000";
    }

    public static String getWebhdfsHost() {
        return "http://localhost:9870";
    }

    public static String getCsvInputPath() {
        return "/";
    }

    public static String getHadoopOutputPath() {
        return "/output/hadoop";
    }

    public static String getSparkOutputPath() {
        return "/output/spark";
    }

    public static String getSparkMaster() {
        return "local[*]";
    }
}
