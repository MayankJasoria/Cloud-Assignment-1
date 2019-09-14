package contracts;

import sqlUtils.Tables;

/**
 * Class that handles returning of the index of a required column from a given table
 * and returning the csv file associated with a given table
 */
public class DBManager implements Cloneable {

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

    public static String getFileName(Tables table) {
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

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
