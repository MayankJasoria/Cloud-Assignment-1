package com.cloud.project.sqlUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class ParseSQL {

    // Complete query
    private String query;

    // gets the columns to be fetched (could be * or list of columns)
    private ArrayList<String> columns;

    // either of INNER_JOIN or GROUP_BY
    private QueryType queryType;

    // stores the tables of interest
    private Tables table1;
    private Tables table2;

    // stores either the column on which join is to be performed, or on which columns grouping is done
    private ArrayList<String> operationColumns;

    // stores the aggregate function to be performed
    private AggregateFunction aggregateFunction;

    private int comparisonNumber;

    private Tables whereTable;
    private String whereColumn;
    private String whereValue;

    private boolean parsed;

    public ParseSQL(String query) {
        this.query = query;
        columns = new ArrayList<>();
        operationColumns = new ArrayList<>();
        aggregateFunction = AggregateFunction.NONE;
        comparisonNumber = -1;
        whereTable = Tables.NONE;
        whereColumn = "";
        whereValue = "";
        parsed = false;
    }

    /**
     * Method which parses the given SQL Query. Prerequisite to all getter calls of this class
     * @throws SQLException in case the SQL query could not be parsed successfully
     */
    private void parseQuery() throws SQLException {
        if (query == null) {
            parsed = false;
            return;
        }

        StringTokenizer tokenizer = new StringTokenizer(query, ", .", false);

        String token = tokenizer.nextToken(); // ignoring first word : SELECT

        // reading required columns (and possibly functions)
        do {
            token = tokenizer.nextToken();
            columns.add(token);
        } while (!token.equalsIgnoreCase("FROM"));

        // this could be *, column names, or even functions on cplumns.
        // We will handle that part later
        columns.remove(columns.size() - 1);

        // getting name of first table
        String table = tokenizer.nextToken();
        if (table.equalsIgnoreCase(Tables.USERS.name())) {
            table1 = Tables.USERS;
        } else if (table.equalsIgnoreCase(Tables.ZIPCODES.name())) {
            table1 = Tables.ZIPCODES;
        } else if (table.equalsIgnoreCase(Tables.MOVIES.name())) {
            table1 = Tables.MOVIES;
        } else if (table.equalsIgnoreCase(Tables.RATING.name())) {
            table1 = Tables.RATING;
        } else {
            throw new SQLException("Table " + table + " does not exist");
        }

        // next 2 tokens will either be group by or inner join.
        // deciding type of query
        String typeOfQuery = tokenizer.nextToken() + "_" + tokenizer.nextToken();
        queryType = (typeOfQuery.equals(QueryType.INNER_JOIN.name())) ? QueryType.INNER_JOIN : QueryType.GROUP_BY;

        if (queryType == QueryType.INNER_JOIN) {
            // get second table for the inner join
            table = tokenizer.nextToken();

            if (table.equalsIgnoreCase(Tables.USERS.name())) {
                table2 = Tables.USERS;
            } else if (table.equalsIgnoreCase(Tables.ZIPCODES.name())) {
                table2 = Tables.ZIPCODES;
            } else if (table.equalsIgnoreCase(Tables.MOVIES.name())) {
                table2 = Tables.MOVIES;
            } else if (table.equalsIgnoreCase(Tables.RATING.name())) {
                table2 = Tables.RATING;
            } else {
                throw new SQLException("Table " + table + " does not exist");
            }

            // get join condition
            token = tokenizer.nextToken(); // ignore "ON"
            token = tokenizer.nextToken(); // ignore table name; this has already been parsed
            operationColumns.add((tokenizer.nextToken()));

            while (!token.equalsIgnoreCase("WHERE")) {
                token = tokenizer.nextToken();
            }

            // get column on which where clause is run
            whereColumn = tokenizer.nextToken(".").trim();
            if (whereColumn.equalsIgnoreCase(Tables.USERS.name())) {
                whereTable = Tables.USERS;
            } else if (whereColumn.equalsIgnoreCase(Tables.ZIPCODES.name())) {
                whereTable = Tables.ZIPCODES;
            } else if (whereColumn.equalsIgnoreCase(Tables.MOVIES.name())) {
                whereTable = Tables.MOVIES;
            } else if (whereColumn.equalsIgnoreCase(Tables.RATING.name())) {
                whereTable = Tables.RATING;
            } else {
                throw new SQLException("table for column of where clause does not exist");
            }

            whereColumn = tokenizer.nextToken("=").substring(1).trim();

            // get value for where clause
            whereValue = tokenizer.nextToken().trim();
        } else {
            table2 = null;
            whereColumn = null;
            whereValue = null;

            // read group by columns
            do {
                token = tokenizer.nextToken();
                operationColumns.add(token);
            } while (!token.equalsIgnoreCase("HAVING"));
            operationColumns.remove(operationColumns.size() - 1);

            // get the aggregate function
            String func = columns.get(columns.size() - 1).split("\\(")[0];
            if (func.equalsIgnoreCase(AggregateFunction.SUM.name())) {
                aggregateFunction = AggregateFunction.SUM;
            } else if (func.equalsIgnoreCase(AggregateFunction.MAX.name())) {
                aggregateFunction = AggregateFunction.MAX;
            } else if (func.equalsIgnoreCase(AggregateFunction.MIN.name())) {
                aggregateFunction = AggregateFunction.MIN;
            } else {
                aggregateFunction = AggregateFunction.COUNT;
            }

            // read condition of having clause; need only the number after the '>' symbol
            tokenizer.nextToken(">");
            comparisonNumber = Integer.parseInt(tokenizer.nextToken());
        }

        if (!tokenizer.hasMoreTokens()) {
            parsed = true;
        } else {
            throw new SQLException("Parsing unsuccessful: tokens remaining to be parsed");
        }
    }

    /**
     * Returns the SQL query string passed for parsing
     *
     * @return The SQL query string
     */
    public String getQuery() {
        return query;
    }

    /**
     * Returns the list of columns which have been selected in SQL query
     * @return {@link ArrayList<String>} either *  for Inner Join, or columns which
     *          have been selected in SQL query (last value is be the aggregate
     *          function used in Group By query)
     * @throws SQLException in case the SQL query could not be parsed successfully
     */
    public ArrayList<String> getColumns() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return columns;
    }

    /**
     * Returns the type of Query
     * @return either of {@link QueryType}.INNER_JOIN or {@link QueryType}.GROUP_BY
     * @throws SQLException in case the SQL query could not be parsed successfully
     */
    public QueryType getQueryType() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return queryType;
    }

    /**
     * Returns the first table of Inner Join, or table for Group By, in SQL Query
     * @return a value from {@link Tables} denoting the first table of Inner Join, or table for Group By, in SQL Query
     * @throws SQLException  in case the SQL query could not be parsed successfully
     */
    public Tables getTable1() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return table1;
    }

    /**
     * Returns the second table of Inner Join in SQL Query
     * @return a value from {@link Tables} denoting the second table of Inner Join in SQL Query
     * @throws SQLException  in case the SQL query could not be parsed successfully
     */
    public Tables getTable2() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return table2;
    }

    /**
     * Returns the list of columns on which SQL operation is to be performed
     * @return {@link ArrayList<String>} columns on which SQL operation is to be performed
     * @throws SQLException in case the SQL query could not be parsed successfully
     */
    public ArrayList<String> getOperationColumns() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return operationColumns;
    }

    /**
     * Returns the number to be compared against for Having clause of given SQL query
     * @return the number to be compared against for the Having clause.
     * @throws SQLException in case the SQL query could not be parsed successfully
     */
    public int getComparisonNumber() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return comparisonNumber;
    }

    public Tables getWhereTable() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return whereTable;
    }

    /**
     * Returns the column name to be tested for in the where clause of the given SQL query.
     *
     * @return column name to be tested for in the where clause.
     * @throws SQLException in case SQL query could not be parsed successfully
     */
    public String getWhereColumn() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return whereColumn;
    }

    /**
     * Returns the type of {@link AggregateFunction} used in the SQL query
     * @return Type of aggregate query used, out of the values of {@link AggregateFunction}
     * @throws SQLException in case SQL query could not be parsed successfully
     */
    public AggregateFunction getAggregateFunction() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return aggregateFunction;
    }

    /**
     * Returns the value of the column to be tested for where clause of the given SQL query.
     *
     * @return value of the column to be tested for where clause
     * @throws SQLException in case SQL query could not be parsed successfully
     */
    public String getWhereValue() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return whereValue;
    }
}
