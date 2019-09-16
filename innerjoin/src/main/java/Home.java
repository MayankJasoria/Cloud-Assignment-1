import jobUtils.GroupBy;
import sqlUtils.ParseSQL;

import java.io.IOException;
import java.sql.SQLException;

public class Home {

    public static void main(String[] args) throws SQLException,
            InterruptedException, IOException, ClassNotFoundException {
        //TODO: Get SQL query from somewhere
        String query1 = "SELECT * FROM Users INNER JOIN Zipcodes ON Users.zipcode = Zipcodes.zipcode WHERE Zipcodes.state = MA";
        String query2 = "SELECT userid, movieid, MAX(rating) FROM Rating GROUP BY movieid, MAX(rating) HAVING MAX(rating)>3";

        // parse query to extract attributes
        ParseSQL parseSQL = new ParseSQL(query2);
        try {
            debugging(parseSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // call required method
        switch (parseSQL.getQueryType()) {
            case GROUP_BY:
                GroupBy.execute(parseSQL);
            case INNER_JOIN:
                // initiate inner join requirements here
        }
    }

    /**
     * Method to be used only for debugging
     *
     * @param parseSQL class being tested
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
        System.out.println();
        System.out.println("Where Clause: " + parseSQL.getWhereClause());
        System.out.println("Having Clause: " + parseSQL.getColumns().get(parseSQL.getColumns().size() - 1)
                + ">" + parseSQL.getComparisonNumber());
    }
}
