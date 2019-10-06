package com.cloud.project.controller;

import com.cloud.project.jobUtils.GroupBy;
import com.cloud.project.jobUtils.InnerJoin;
import com.cloud.project.models.InputModel;
import com.cloud.project.models.OutputModel;
import com.cloud.project.scala_queries.SparkGroupBy;
import com.cloud.project.scala_queries.SparkInnerJoin;
import com.cloud.project.sqlUtils.ParseSQL;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Class which acts as the home class for the application since all relevant API routes are managed here
 */
@Path("/")
public class Home {

    /**
     * main method, relevant for debugging purposes in this application
     *
     * @param args Array of inputs from the command line. Not used in this method
     * @throws SQLException           if the SQL query could not be parsed successfully
     * @throws InterruptedException   if the Hadoop/Spark jobs throw this exception
     * @throws IOException            if the Hadoop/Spark jobs throw this exception
     * @throws ClassNotFoundException if the Hadoop/Spark jobs throw this exception
     */
    public static void main(String[] args) throws SQLException, InterruptedException, IOException, ClassNotFoundException {
        String query1 = "SELECT * FROM  Movies INNER JOIN Rating ON Movies.movieid = Rating.movieid WHERE Rating.rating=4";
        String query2 = "SELECT userid, sum(rating) FROM Rating GROUP BY userid HAVING SUM(rating)>0";

        // parse query to extract attributes
        ParseSQL parseSQL = new ParseSQL(query1);
        try {
            debugging(parseSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // creating an instance of OutputModel to store all relevant outputs
        OutputModel outputModel = null;

        // call required method
        switch (parseSQL.getQueryType()) {
            case GROUP_BY:
                // query of type group by
                outputModel = GroupBy.execute(parseSQL);
                SparkGroupBy.execute(parseSQL, outputModel);
                break;
            case INNER_JOIN:
                // query of type inner join
                outputModel = InnerJoin.execute(parseSQL);
                SparkInnerJoin.execute(parseSQL, outputModel);
        }

        // display output
        System.out.println("FirstMapperExecutionPlan: " + outputModel.getFirstMapperPlan());
        System.out.println("SecondMapperExecutionPlan: " + outputModel.getSecondMapperPlan());
        System.out.println("GroupByMapperExecutionPlan: " + outputModel.getGroupByMapperPlan());
        System.out.println("GroupByReducerExecutionPlan: " + outputModel.getGroupByReducerPlan());
        System.out.println("InnerJoinReducerExecutionPlan: " + outputModel.getInnerJoinReducerPlan());
        System.out.println("Hadoop Execution time: " + outputModel.getHadoopExecutionTime());
        System.out.println("Hadoop Output URL: " + outputModel.getHadoopOutputUrl());
        System.out.println("Spark Execution Time: " + outputModel.getSparkExecutionTime());
        System.out.println("Spark Output URL: " + outputModel.getSparkOutputUrl());
        System.out.println("Spark Plan: " + outputModel.getSparkPlan());
    }

    /**
     * Method to be used only for debugging. It prints the various tokens as extracted
     * by the SQL Parser (ParseSQL class)
     *
     * @param parseSQL instance of parseSQL
     * @throws SQLException if SQL query was not parsed successfully
     */
    private static void debugging(ParseSQL parseSQL) throws SQLException {
        System.out.println("Query: " + parseSQL.getQuery());
        System.out.println("\nColumns:");
        for (String column : parseSQL.getColumns()) {
            System.out.println(column);
        }
        System.out.println();
        System.out.println("Table 1: " + parseSQL.getTable1());
        System.out.println("Query type: " + parseSQL.getQueryType().name());
        System.out.println("Table 2: " + parseSQL.getTable2());
        System.out.println("\nOperation Columns: ");
        for (String operationColumns : parseSQL.getOperationColumns()) {
            System.out.println(operationColumns);
        }
        System.out.println("AggregateFunction: " + parseSQL.getAggregateFunction().name());
        System.out.println();
        System.out.println("Where Clause: " + parseSQL.getWhereTable().name() + "." + parseSQL.getWhereColumn() + "=" + parseSQL.getWhereValue());
//        System.out.println(DBManager.getColumnIndex(parseSQL.getWhereTable(), parseSQL.getWhereColumn()));
        System.out.println("Having Clause: " + parseSQL.getColumns().get(parseSQL.getColumns().size() - 1)
                + ">" + parseSQL.getComparisonNumber());
    }

    /**
     * Method to be used to test if the API is live and accepting requests.
     * <p>
     * If it works correctly it will display "Hello World!" as an output.
     * <br>
     * Otherwise the corresponding errors will be displayed.
     * </p>
     *
     * @return The String "Hello World!"
     */
    @GET
    @Path("test")
    public String helloWorld() {
        return "Hello World!";
    }

    /**
     * Method that accepts all POST requests to the /api/query endpoint.
     * <p>
     *     This method accepts the POST request and parses the given SQL query as part of the
     *     body of the request, which is received as {@link InputModel}. Thereafter, depending
     *     on the type of query as defined by {@link com.cloud.project.sqlUtils.QueryType},
     *     the appropriate Hadoop and Spark jobs are launched. Once the results of the jobs are
     *     available, an instance of {@link OutputModel} is returned which is eventually serialized
     *     by {@link com.owlike.genson.Genson} and returned as a JSON output.
     * </p>
     * @param inputModel deserailized JSON input bearing the body of the request
     * @return instance of {@link OutputModel} to be serialized as a JSON output
     * @throws SQLException if the given SQL query is not parsed successfully
     * @throws InterruptedException if the Hadoop/Spark jobs encounter this exception
     * @throws IOException if the Hadoop/Spark jobs encounter this exception
     * @throws ClassNotFoundException if the Hadoop/Spark jobs encounter this exception
     */
    @POST
    @Path("query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public OutputModel resultOfQuery(InputModel inputModel)
            throws SQLException, InterruptedException, IOException, ClassNotFoundException {

        // parse query to extract attributes
        ParseSQL parseSQL = new ParseSQL(inputModel.getQuery());
        try {
            debugging(parseSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        OutputModel outputModel = null;

        // call required method
        switch (parseSQL.getQueryType()) {
            case GROUP_BY:
                // query of type group by
                outputModel = GroupBy.execute(parseSQL);
                SparkGroupBy.execute(parseSQL, outputModel);
                break;
            case INNER_JOIN:
                // query of type inner join
                outputModel = InnerJoin.execute(parseSQL);
                SparkInnerJoin.execute(parseSQL, outputModel);
        }

        return outputModel;
    }
}
