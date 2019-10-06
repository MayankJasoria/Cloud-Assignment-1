package com.cloud.project;

/**
 * Class which behaves as a configuration file for global attributes
 */
public class Globals {

    /**
     * Returns the URL of the Hadoop Namenode
     *
     * @return String depicting URL of the Hadoop Namenode
     */
    public static String getNamenodeUrl() {
        return "hdfs://localhost:9000";
    }

    /**
     * Returns the URL for Hadoop WebHDFS
     *
     * @return String depicting URL of Hadoop WebHDFS
     */
    public static String getWebhdfsHost() {
        return "http://localhost:9870";
    }

    /**
     * Returns the relative HDFS input path of the CSV files being used as tables.
     * @return String depicting relative input path of the CSV files being used as tables.
     */
    public static String getCsvInputPath() {
        return "/";
    }

    /**
     * Returns the relative HDFS output path of the Hadoop Job
     * @return String depicting relative HDFS output path of the Hadoop Job
     */
    public static String getHadoopOutputPath() {
        return "/output/hadoop";
    }

    /**
     * Returns the relative HDFS output path of the Spark Job
     * @return String depicting relative HDFS output path of the Spark Job
     */
    public static String getSparkOutputPath() {
        return "/output/spark";
    }

    /**
     * Returns the Spark master configuration to be used
     * @return Spark master configuration to be used
     */
    public static String getSparkMaster() {
        return "local[*]";
    }
}
