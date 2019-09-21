package cc.projects.mrutils.contracts;

import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Rating"
 */
public class RatingsContract {

    private static final HashMap<String, Integer> map;

    static {
        map = new HashMap<>();
        map.put("userid", 0);
        map.put("movieid", 1);
        map.put("rating", 2);
        map.put("timestamp", 3);
    }

    private RatingsContract() {
        // ensuring that a constructor for this class cannot be created
    }

    public static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Rating table");
    }

    public static String getFileName() {
        return "rating.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }

}
