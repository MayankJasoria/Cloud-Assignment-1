package contracts;

import sqlUtils.Tables;

/**
 * Class that handles returning of the index of a required column from a given table
 * and returning the csv file associated with a given table
 */
public class DBManager implements Cloneable {

    private DBManager() {
        // private constructor to restrict object creation
    }

    /**
     * Method that returns the index of a given column
     *
     * @param table  The table in which the column is present
     * @param column The column whose index is required
     * @return index of the column
     * @throws IllegalArgumentException when either the table or the column are invalid
     */
    public static int getColumnIndex(Tables table, String column) throws IllegalArgumentException {
        switch (table) {
            case USERS:
                return UsersContract.getColumnIndex(column);
            case ZIPCODES:
                return ZipcodesContract.getColumnIndex(column);
            case MOVIES:
                return MoviesContract.getColumnIndex(column);
            case RATING:
                return RatingsContract.getColumnIndex(column);
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }

    /**
     * Method that returns the name of the csv file corresponding to a given table
     *
     * @param table The table corresponding to which the csv file name is required
     * @return csv file name for the given table
     * @throws IllegalArgumentException when the table name is invalid (highly unlikely)
     */
    public static String getFileName(Tables table) throws IllegalArgumentException {
        switch (table) {
            case USERS:
                return UsersContract.getFileName();
            case ZIPCODES:
                return ZipcodesContract.getFileName();
            case MOVIES:
                return MoviesContract.getFileName();
            case RATING:
                return RatingsContract.getFileName();
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }

    /**
     * Given a pair of tables, returns the join key.
     *
     * @param table1 table name
     * @param table2 table name
     * @return index of the join key
     * @throws IllegalArgumentException when the table in invalid
     */

    public static String getJoinKey(Tables table1, Tables table2)
            throws IllegalArgumentException {

        switch (table1.getValue() ^ table2.getValue()) {
            case 1 ^ 2:
                return "zipcode";
            case 1 ^ 3:
                return null;
            case 1 ^ 4:
                return "userid";
            case 2 ^ 3:
                return null;
            case 2 ^ 4:
                return null;
            case 3 ^ 4:
                return "movieid";
            default:
                throw new IllegalArgumentException("Table " + table1.name().toLowerCase()
                        + " or " + table2.name().toLowerCase() + " not exist");
        }
    }

//     /**
//      * Returns contract class given Table
//      *
//      * @param table The table corresponding to which the csv file name is required
//      * @return csv file name for the given table
//      * @throws IllegalArgumentException when the table name is invalid (highly unlikely)
//      */
//     public static String getFileName(Tables table) throws IllegalArgumentException {
//         switch (table) {
//             case USERS:
//                 return UsersContract.getFileName();
//             case ZIPCODES:
//                 return ZipcodesContract.getFileName();
//             case MOVIES:
//                 return MoviesContract.getFileName();
//             case RATING:
//                 return RatingsContract.getFileName();
//             default:
//                 throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
//         }
//     }



    /**
     * Method overridden to ensure that this class is not cloned
     * @return null
     * @throws CloneNotSupportedException since this class cannot be cloned
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
