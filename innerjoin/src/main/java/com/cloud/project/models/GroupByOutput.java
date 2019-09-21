package com.cloud.project.models;

public class GroupByOutput implements OutputModel {

    // hadoop parameters
    private String hadoopExecutionTime;
    private String GroupByMapperPlan;
    private String GroupByReducerPlan;
    private String hadoopOutputUrl;

    // spark parameters
    private String sparkExecutionTime;
    private String sparkPlan;
    private String sparkOutputUrl;

    // TODO: specify actual file output

    public String getHadoopExecutionTime() {
        return hadoopExecutionTime;
    }

    public void setHadoopExecutionTime(String hadoopExecutionTime) {
        this.hadoopExecutionTime = hadoopExecutionTime;
    }

    public String getGroupByMapperPlan() {
        return GroupByMapperPlan;
    }

    public void setGroupByMapperPlan(String groupByMapperPlan) {
        GroupByMapperPlan = groupByMapperPlan;
    }

    public String getGroupByReducerPlan() {
        return GroupByReducerPlan;
    }

    public void setGroupByReducerPlan(String groupByReducerPlan) {
        GroupByReducerPlan = groupByReducerPlan;
    }

    public String getHadoopOutputUrl() {
        return hadoopOutputUrl;
    }

    public void setHadoopOutputUrl(String hadoopOutputUrl) {
        this.hadoopOutputUrl = hadoopOutputUrl;
    }

    public String getSparkExecutionTime() {
        return sparkExecutionTime;
    }

    public void setSparkExecutionTime(String sparkExecutionTime) {
        this.sparkExecutionTime = sparkExecutionTime;
    }

    public String getSparkPlan() {
        return sparkPlan;
    }

    public void setSparkPlan(String sparkPlan) {
        this.sparkPlan = sparkPlan;
    }

    public String getSparkOutputUrl() {
        return sparkOutputUrl;
    }

    public void setSparkOutputUrl(String sparkOutputUrl) {
        this.sparkOutputUrl = sparkOutputUrl;
    }
}
