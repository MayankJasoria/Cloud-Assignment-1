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
    private String table1;
    private String table2;

    // stores either the column on which join is to be performed, or on which columns grouping is done
    private ArrayList<String> operationColumns;

    private int comparisonNumber;

    private String whereClause;

    private boolean parsed;

    public ParseSQL(String query) {
        this.query = query;
        columns = new ArrayList<>();
        operationColumns = new ArrayList<>();
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

        StringTokenizer tokenizer = new StringTokenizer(query, ", .>", false);

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
        table1 = tokenizer.nextToken();

        // next 2 tokens will either be group by or inner join.
        // deciding type of query
        String typeOfQuery = tokenizer.nextToken() + "_" + tokenizer.nextToken();
        queryType = (typeOfQuery.equals(QueryType.INNER_JOIN.name())) ? QueryType.INNER_JOIN : QueryType.GROUP_BY;

        if (queryType == QueryType.INNER_JOIN) {
            // get second table for the inner join
            table2 = tokenizer.nextToken();

            // get join condition
            token = tokenizer.nextToken(); // ignore table name
            operationColumns.add((tokenizer.nextToken()));

            while (!token.equalsIgnoreCase("WHERE")) {
                token = tokenizer.nextToken();
            }

            while (tokenizer.hasMoreTokens()) {
                // replace with string buffer if needed later
                whereClause += " " + tokenizer.nextToken();
            }
        } else {
            table2 = null;

            // read group by columns
            do {
                token = tokenizer.nextToken();
                operationColumns.add(token);
            } while (!token.equalsIgnoreCase("HAVING"));

            // read condition of having clause; need only the number after the '>' symbol
            tokenizer.nextToken();
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

    public String getTable1() throws SQLException {
        if (!parsed) {
            parseQuery();
        }
        return table1;
    }

    public String getTable2() throws SQLException {
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
}
