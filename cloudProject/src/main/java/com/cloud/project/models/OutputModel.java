package com.cloud.project.models;

/**
 * POJO used to serialize app outputs to JSON to be returned as response for each request to /api/query
 */
public class OutputModel {

    // hadoop parameters
    private String hadoopExecutionTime;
    private String GroupByMapperPlan;
    private String GroupByReducerPlan;
    private String FirstMapperPlan;
    private String SecondMapperPlan;
    private String InnerJoinReducerPlan;
    private String hadoopOutputUrl;

    // spark parameters
    private String sparkExecutionTime;
    private String sparkPlan;
    private String sparkOutputUrl;

    /**
     * Constructor to set default values for all parameters.
     * <br>
     * (All parameters which are null are ignored during serialization to JSON)
     */
    public OutputModel() {
        hadoopExecutionTime = null;
        GroupByMapperPlan = null;
        GroupByReducerPlan = null;
        FirstMapperPlan = null;
        SecondMapperPlan = null;
        InnerJoinReducerPlan = null;
        hadoopOutputUrl = null;
        sparkExecutionTime = null;
        sparkPlan = null;
        sparkOutputUrl = null;
    }

    /**
     * Returns the execution time taken by the Hadoop Job.
     *
     * @return String depicting execution time taken by the Hadoop Job.
     */
    public String getHadoopExecutionTime() {
        return hadoopExecutionTime;
    }

    /**
     * Sets the execution time taken by the Hadoop Job
     *
     * @param hadoopExecutionTime String denoting the execution time of the Hadoop Job
     */
    public void setHadoopExecutionTime(String hadoopExecutionTime) {
        this.hadoopExecutionTime = hadoopExecutionTime;
    }

    /**
     * Returns the execution plan of Mapper of {@link com.cloud.project.jobUtils.GroupBy}
     * @return String denoting the execution plan of the GroupByMapper
     */
    public String getGroupByMapperPlan() {
        return GroupByMapperPlan;
    }

    /**
     * Sets the execution plan of Reducer of {@link com.cloud.project.jobUtils.GroupBy}
     * @param groupByMapperPlan String denoting the execution plan of the GroupByMapper
     */
    public void setGroupByMapperPlan(String groupByMapperPlan) {
        GroupByMapperPlan = groupByMapperPlan;
    }

    /**
     * Returns the execution plan of Reducer of {@link com.cloud.project.jobUtils.GroupBy}
     * @return String denoting the execution plan of the GroupByReducer
     */
    public String getGroupByReducerPlan() {
        return GroupByReducerPlan;
    }

    /**
     * Sets the execution plan of Reducer of {@link com.cloud.project.jobUtils.GroupBy}
     * @param groupByReducerPlan String denoting the execution plan of the GroupByReducer
     */
    public void setGroupByReducerPlan(String groupByReducerPlan) {
        GroupByReducerPlan = groupByReducerPlan;
    }

    /**
     * Returns the execution plan of FirstReducer of {@link com.cloud.project.jobUtils.GroupBy}
     * @return String denoting the execution plan of the FirstReducer
     */
    public String getFirstMapperPlan() {
        return FirstMapperPlan;
    }

    /**
     * Sets the execution plan of FirstMapper of {@link com.cloud.project.jobUtils.GroupBy}
     * @param firstMapperPlan String denoting the execution plan of the FirstMapper
     */
    public void setFirstMapperPlan(String firstMapperPlan) {
        this.FirstMapperPlan = firstMapperPlan;
    }

    /**
     * Returns the execution plan of SecondMapper of {@link com.cloud.project.jobUtils.GroupBy}
     * @return String denoting the execution plan of the SecondMapper
     */
    public String getSecondMapperPlan() {
        return SecondMapperPlan;
    }

    /**
     * Sets the execution plan of SecondMapper of {@link com.cloud.project.jobUtils.GroupBy}
     * @param secondMapperPlan String denoting the execution plan of the SecondMapper
     */
    public void setSecondMapperPlan(String secondMapperPlan) {
        this.SecondMapperPlan = secondMapperPlan;
    }

    /**
     * Returns the execution plan of InnerJoinReducer of {@link com.cloud.project.jobUtils.GroupBy}
     * @return String denoting the execution plan of the InnerJoinReducer
     */
    public String getInnerJoinReducerPlan() {
        return InnerJoinReducerPlan;
    }

    /**
     * Sets the execution plan of InnerJoinReducer of {@link com.cloud.project.jobUtils.GroupBy}
     * @param innerJoinReducerPlan String denoting the execution plan of the InnerJoinReducer
     */
    public void setInnerJoinReducerPlan(String innerJoinReducerPlan) {
        this.InnerJoinReducerPlan = innerJoinReducerPlan;
    }

    /**
     * Returns the WebHDFS URL for the output of the Hadoop Job
     * @return String depicting the WebHDFS URL for the output of the Hadoop Job
     */
    public String getHadoopOutputUrl() {
        return hadoopOutputUrl;
    }

    /**
     * Sets the WebHDFS URL for the output of the Hadoop Job
     * @param hadoopOutputUrl String depicting the WebHDFS URL for the output of the Hadoop Job
     */
    public void setHadoopOutputUrl(String hadoopOutputUrl) {
        this.hadoopOutputUrl = hadoopOutputUrl;
    }

    /**
     * Returns the execution time taken by the Spark Job.
     * @return String depicting execution time taken by the Spark Job.
     */
    public String getSparkExecutionTime() {
        return sparkExecutionTime;
    }

    /**
     * Sets the execution time taken by the Hadoop Job.
     * @param sparkExecutionTime String depicting execution time taken by the Hadoop Job.
     */
    public void setSparkExecutionTime(String sparkExecutionTime) {
        this.sparkExecutionTime = sparkExecutionTime;
    }

    /**
     * Returns the execution plan (transformations) of the Spark Job
     * @return String depicting the execution plan (transformations) of the Spark Job
     */
    public String getSparkPlan() {
        return sparkPlan;
    }

    /**
     * Sets the execution plan (transformations) of the Spark Job
     * @param sparkPlan  String depicting the execution plan (transformations) of the Spark Job
     */
    public void setSparkPlan(String sparkPlan) {
        this.sparkPlan = sparkPlan;
    }

    /**
     * Returns the WebHDFS url for the output of the Spark Job
     * @return String depicting the WebHDFS url for the output of the Spark Job
     */
    public String getSparkOutputUrl() {
        return sparkOutputUrl;
    }

    /**
     * Returns the WebHDFS url for the output of the Spark Job
     * @param sparkOutputUrl String depicting the WebHDFS url for the output of the Spark Job
     */
    public void setSparkOutputUrl(String sparkOutputUrl) {
        this.sparkOutputUrl = sparkOutputUrl;
    }
}
