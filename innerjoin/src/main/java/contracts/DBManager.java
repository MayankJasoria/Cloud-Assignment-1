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
            case RATINGS:
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
            case RATINGS:
                return RatingsContract.getFileName();
            default:
                throw new IllegalArgumentException("Table " + table.name().toLowerCase() + " does not exist");
        }
    }

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
