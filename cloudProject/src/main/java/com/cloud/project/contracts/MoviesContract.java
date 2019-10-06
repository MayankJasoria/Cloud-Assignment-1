package com.cloud.project.contracts;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Movies"
 */
public class MoviesContract implements Cloneable {

    private static final HashMap<String, Integer> map;
    private static final ArrayList<String> indToCol;

    // initializing the required static variables
    static {
        map = new HashMap<>();
        map.put("movieid", 0);
        map.put("title", 1);
        map.put("releasedate", 2);
        map.put("unknown", 3);
        map.put("Action", 4);
        map.put("Adventure", 5);
        map.put("Animation", 6);
        map.put("Children", 7);
        map.put("Comedy", 8);
        map.put("Crime", 9);
        map.put("Documentary", 10);
        map.put("Drama", 11);
        map.put("Fantasy", 12);
        map.put("Film_Noir", 13);
        map.put("Horror", 14);
        map.put("Musical", 15);
        map.put("Mystery", 16);
        map.put("Romance", 17);
        map.put("Sci_Fi", 18);
        map.put("Thriller", 19);
        map.put("War", 20);
        map.put("Western", 21);
        map.put("Rating", 22);

        indToCol = new ArrayList<>();
        indToCol.add("movieid");
        indToCol.add("title");
        indToCol.add("releasedate");
        indToCol.add("unknown");
        indToCol.add("Action");
        indToCol.add("Adventure");
        indToCol.add("Animation");
        indToCol.add("Children");
        indToCol.add("Comedy");
        indToCol.add("Crime");
        indToCol.add("Documentary");
        indToCol.add("Drama");
        indToCol.add("Fantasy");
        indToCol.add("Film_Noir");
        indToCol.add("Horror");
        indToCol.add("Musical");
        indToCol.add("Mystery");
        indToCol.add("Romance");
        indToCol.add("Sci_Fi");
        indToCol.add("Thriller");
        indToCol.add("War");
        indToCol.add("Western");
        indToCol.add("Rating");
    }

    private MoviesContract() {
        // making constructor private to restrict external access
    }

    /**
     * Method to return the column index in the movies table from the given column name
     *
     * @param column String denoting the column name
     * @return index of the column in the movies table
     * @throws IllegalArgumentException when the provided column name is invalid
     */
    static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Movies table");
    }

    /**
     * Method to return the column name in the movies table from the given column index
     *
     * @param index index of the column
     * @return name of the column
     * @throws IllegalArgumentException when the provided column index is invalid
     */
    static String getColumnFromIndex(int index) throws IllegalArgumentException {
        if (!(index > indToCol.size() - 1)) {
            return indToCol.get(index);
        }
        throw new IllegalArgumentException("Given column does not exist in Movies table");
    }

    /**
     * Method to return the number of columns in the movies table
     * @return number of column in the movies table
     */
    static int getNumColumns() {
        return indToCol.size();
    }

    /**
     * Method to return the name of the actual file which is used as the movies table
     * @return name of the actual file for the movies table
     */
    static String getFileName() {
        return "movies.csv";
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
