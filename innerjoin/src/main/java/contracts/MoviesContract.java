package contracts;

import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Movies"
 */
public class MoviesContract implements Cloneable {

    private static final HashMap<String, Integer> map;

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
    }

    private MoviesContract() {
        // making constructor private to restrict external access
    }

    public static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Movies table");
    }

    public static String getFileName() {
        return "movies.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
