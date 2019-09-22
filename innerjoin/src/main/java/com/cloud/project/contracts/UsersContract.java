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

    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Users table");
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
        return "users.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }

}
