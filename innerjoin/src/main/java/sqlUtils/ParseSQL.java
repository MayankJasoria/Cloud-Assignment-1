package sqlUtils;

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

    private String whereClause;

    private boolean parsed;

    public ParseSQL(String query) {
        this.query = query;
        columns = new ArrayList<>();
        operationColumns = new ArrayList<>();
        aggregateFunction = AggregateFunction.NONE;
        comparisonNumber = -1;
        whereClause = "";
        parsed = false;
    }

    /**
     * Method which parses the given SQL Query. Prerequisite to all getter calls of this class
     *
     * @throws SQLException if parsing failed
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
        } else if (table.equalsIgnoreCase(Tables.RATINGS.name())) {
            table1 = Tables.RATINGS;
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
            } else if (table.equalsIgnoreCase(Tables.RATINGS.name())) {
                table2 = Tables.RATINGS;
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

            while (tokenizer.hasMoreTokens()) {
                // replace with string buffer if needed later
                whereClause += " " + tokenizer.nextToken(" ");
            }
        } else {
            table2 = null;
            whereClause = null;

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

    public String getQuery() {
        return query;
    }

    public ArrayList<String> getColumns() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return columns;
    }

    public QueryType getQueryType() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return queryType;
    }

    public Tables getTable1() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return table1;
    }

    public Tables getTable2() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return table2;
    }

    public ArrayList<String> getOperationColumns() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return operationColumns;
    }

    public int getComparisonNumber() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return comparisonNumber;
    }

    public String getWhereClause() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return whereClause;
    }

    public AggregateFunction getAggregateFunction() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return aggregateFunction;
    }
}
