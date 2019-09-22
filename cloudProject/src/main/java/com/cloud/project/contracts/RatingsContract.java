package com.cloud.project.contracts;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Rating"
 */
public class RatingsContract {

    private static final HashMap<String, Integer> map;
    private static final ArrayList<String> indToCol;

    static {
        map = new HashMap<>();
        map.put("userid", 0);
        map.put("movieid", 1);
        map.put("rating", 2);
        map.put("timestamp", 3);
    }

    static {
        indToCol = new ArrayList<>();
        indToCol.add("userid");
        indToCol.add("movieid");
        indToCol.add("rating");
        indToCol.add("timestamp");
    }

    private RatingsContract() {
        // ensuring that a constructor for this class cannot be created
    }

    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Rating table");
    }

    static String getColumnFromIndex(int index) throws IllegalArgumentException {
        if (!(index > indToCol.size() - 1)) {
            return indToCol.get(index);
        }
        throw new IllegalArgumentException("Given column does not exist in Movies table");
    }

    static int getNumColumns() {
        return indToCol.size();
    }

    static String getFileName() {
        return "rating.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }

}
