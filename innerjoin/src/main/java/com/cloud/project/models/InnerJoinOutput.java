package com.cloud.project.models;

public class InnerJoinOutput {

    // hadoop parameters
    private String hadoopExecutionTime;
    private String FirstMapperPlan;
    private String SecondMapperPlan;
    private String InnerJoinReducerPlan;
    private String hadoopOutputUrl;

    // spark parameters
    private String sparkExecutionTime;
    private String sparkOutputUrl;

    // TODO: specify actual file output

    public String getHadoopExecutionTime() {
        return hadoopExecutionTime;
    }

    public void setHadoopExecutionTime(String hadoopExecutionTime) {
        this.hadoopExecutionTime = hadoopExecutionTime;
    }

    public String getFirstMapperPlan() {
        return FirstMapperPlan;
    }

    public void setFirstMapperPlan(String firstMapperPlan) {
        this.FirstMapperPlan = firstMapperPlan;
    }

    public String getSecondMapperPlan() {
        return SecondMapperPlan;
    }

    public void setSecondMapperPlan(String secondMapperPlan) {
        this.SecondMapperPlan = secondMapperPlan;
    }

    public String getInnerJoinReducerPlan() {
        return InnerJoinReducerPlan;
    }

    public void setInnerJoinReducerPlan(String innerJoinReducerPlan) {
        this.InnerJoinReducerPlan = innerJoinReducerPlan;
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

    public String getSparkOutputUrl() {
        return sparkOutputUrl;
    }

    public void setSparkOutputUrl(String sparkOutputUrl) {
        this.sparkOutputUrl = sparkOutputUrl;
    }
}
