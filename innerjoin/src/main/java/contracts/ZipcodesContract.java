package contracts;

import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Zipcodes"
 */
class ZipcodesContract implements Cloneable {

    private static final HashMap<String, Integer> map;

    static {
        map = new HashMap<>();
        map.put("zipcode", 0);
        map.put("zipcodetype", 1);
        map.put("city", 2);
        map.put("state", 3);
    }

    private ZipcodesContract() {
        // ensuring that a constructor for this class cannot be created
    }

    public static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Users table");
    }

    public static String getFileName() {
        return "zipcodes.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }
}
