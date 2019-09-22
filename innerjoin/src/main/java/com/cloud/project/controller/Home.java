package com.cloud.project.controller;

import com.cloud.project.jobUtils.GroupBy;
import com.cloud.project.models.GroupByOutput;
import com.cloud.project.models.InputModel;
import com.cloud.project.sqlUtils.ParseSQL;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.SQLException;

@Path("/")
public class Home {

    @GET
    @Path("test")
    public String helloWorld() {
        return "Hello World!";
    }

    @POST
    @Path("query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GroupByOutput resultOfQuery(InputModel inputModel) throws SQLException,
            InterruptedException, IOException, ClassNotFoundException {
        //TODO: Get SQL query from somewhere
//        String query1 = "SELECT * FROM Users INNER JOIN Zipcodes ON Users.zipcode = Zipcodes.zipcode WHERE Zipcodes.state = MA";
//        String query2 = "SELECT userid, movieid, count(rating) FROM Rating GROUP BY userid, movieid HAVING COUNT(rating)>0";

        // parse query to extract attributes
        ParseSQL parseSQL = new ParseSQL(inputModel.getQuery());
        try {
            debugging(parseSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        GroupByOutput outputModel = null;

        // call required method
        switch (parseSQL.getQueryType()) {
            case GROUP_BY:
                outputModel = GroupBy.execute(parseSQL);
//                SparkGroupBy.execute(parseSQL, outputModel);
//                break;
            case INNER_JOIN:
//                outputModel = InnerJoin.execute(parseSQL);
//                SparkInnerJoin.execute(parseSQL, outputModel);
        }

        return outputModel;
    }

    public static void main(String[] args) throws SQLException, InterruptedException, IOException, ClassNotFoundException {
        String query2 = "SELECT userid, movieid, count(rating) FROM Rating GROUP BY userid, movieid HAVING COUNT(rating)>0";

        // parse query to extract attributes
        ParseSQL parseSQL = new ParseSQL(query2);
        try {
            debugging(parseSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        GroupByOutput outputModel = null;

        // call required method
        switch (parseSQL.getQueryType()) {
            case GROUP_BY:
                outputModel = GroupBy.execute(parseSQL);
//                SparkGroupBy.execute(parseSQL, outputModel);
//                break;
            case INNER_JOIN:
//                outputModel = InnerJoin.execute(parseSQL);
//                SparkInnerJoin.execute(parseSQL, outputModel);
        }

        System.out.println("MapperExecutionPlan: " + outputModel.getGroupByMapperPlan());
        System.out.println("ReducerExecutionPlan: " + outputModel.getGroupByReducerPlan());
        System.out.println("Execution time: " + outputModel.getHadoopExecutionTime());
        System.out.println("Output URL: " + outputModel.getHadoopOutputUrl());

    }

    /**
     * Method to be used only for debugging
     *
     * @param parseSQL class being tested
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
        System.out.println("Where Clause: " + parseSQL.getWhereColumn() + "=" + parseSQL.getWhereValue());
        System.out.println("Having Clause: " + parseSQL.getColumns().get(parseSQL.getColumns().size() - 1)
                + ">" + parseSQL.getComparisonNumber());
    }
}
