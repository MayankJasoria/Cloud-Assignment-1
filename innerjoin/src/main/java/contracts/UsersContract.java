package contracts;

import java.util.HashMap;

/**
 * Class to manage mapping of columns to their indices in the csv file.
 * Also returns the actual name of the csv file for the table "Users"
 */
class UsersContract implements Cloneable {

    private static final HashMap<String, Integer> map;

    static {
        map = new HashMap<>();
        map.put("userid", 0);
        map.put("age", 1);
        map.put("gender", 2);
        map.put("occupation", 3);
        map.put("zipcode", 4);
    }

    private UsersContract() {
        // ensuring that a constructor for this class cannot be created
    }

    public static int getColumnIndex(String column) throws IllegalArgumentException {
        if (map.containsKey(column)) {
            return map.get(column);
        }
        throw new IllegalArgumentException("Given column does not exist in Users table");
    }

    public static String getFileName() {
        return "users.csv";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("This class cannot be cloned");
    }

}
