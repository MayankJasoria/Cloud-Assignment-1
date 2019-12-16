package com.cloud.project.contracts;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Users"
 */
class UsersContract implements Cloneable {

    private static final HashMap<String, Integer> map;
    private static final ArrayList<String> indToCol;

    // initializing the required static variables
    static {
        map = new HashMap<>();
        map.put("userid", 0);
        map.put("age", 1);
        map.put("gender", 2);
        map.put("occupation", 3);
        map.put("zipcode", 4);

        indToCol = new ArrayList<>();
        indToCol.add("userid");
        indToCol.add("age");
        indToCol.add("gender");
        indToCol.add("occupation");
        indToCol.add("zipcode");
    }

    private UsersContract() {
        // ensuring that a constructor for this class cannot be created
    }

    /**
     * Method to return the column index in the users table from the given column name
     *
     * @param column String denoting the column name
     * @return index of the column in the users table
     * @throws IllegalArgumentException when the provided column name is invalid
     */
    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Users table");
    }

    /**
     * Method to return the column name in the users table from the given column index
     *
     * @param index index of the column
     * @return name of the column
     * @throws IllegalArgumentException when the provided column index is invalid
     */
    static String getColumnFromIndex(int index) throws IllegalArgumentException {
        if (!(index > indToCol.size() - 1)) {
            return indToCol.get(index);
        }
        throw new IllegalArgumentException("Given column does not exist in Users table");
    }

    /**
     * Method to return the number of columns in the users table
     * @return number of column in the users table
     */
    static int getNumColumns() {
        return indToCol.size();
    }

    /**
     * Method to return the name of the actual file which is used as the users table
     * @return name of the actual file for the users table
     */
    static String getFileName() {
        return "users.csv";
    }

    /**
     * Method overridden to ensure that this class is not cloned
     *
     * @return null
     * @throws CloneNotSupportedException since this class cannot be cloned
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }

}
