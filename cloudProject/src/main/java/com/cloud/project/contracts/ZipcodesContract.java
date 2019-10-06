package com.cloud.project.contracts;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Zipcodes"
 */
class ZipcodesContract implements Cloneable {

    private static final HashMap<String, Integer> map;
    private static final ArrayList<String> indToCol;

    // initializing the required static variables
    static {
        map = new HashMap<>();
        map.put("zipcode", 0);
        map.put("zipcodetype", 1);
        map.put("city", 2);
        map.put("state", 3);

        indToCol = new ArrayList<>();
        indToCol.add("zipcode");
        indToCol.add("zipcodetype");
        indToCol.add("city");
        indToCol.add("state");
    }

    private ZipcodesContract() {
        // ensuring that a constructor for this class cannot be created
    }

    /**
     * Method to return the column index in the zipcodes table from the given column name
     *
     * @param column String denoting the column name
     * @return index of the column in the zipcodes table
     * @throws IllegalArgumentException when the provided column name is invalid
     */
    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column.trim())) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Zipcodes table");
    }

    /**
     * Method to return the column name in the zipcodes table from the given column index
     *
     * @param index index of the column
     * @return name of the column
     * @throws IllegalArgumentException when the provided column index is invalid
     */
    static String getColumnFromIndex(int index) throws IllegalArgumentException {
        if (!(index > indToCol.size() - 1)) {
            return indToCol.get(index);
        }
        throw new IllegalArgumentException("Given column does not exist in Zipcodes table");
    }

    /**
     * Method to return the number of columns in the zipcodes table
     * @return number of column in the zipcodes table
     */
    static int getNumColumns() {
        return indToCol.size();
    }

    /**
     * Method to return the name of the actual file which is used as the zipcodes table
     * @return name of the actual file for the zipcodes table
     */
    static String getFileName() {
        return "zipcodes.csv";
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
