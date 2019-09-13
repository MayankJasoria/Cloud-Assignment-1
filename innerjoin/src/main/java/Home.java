public class Home {

    public static void main(String[] args) {
        //TODO: Get SQL query from somewhere
        String query = "SELECT * FROM Users INNER JOIN Zipcodes ON Users.zipcode = Zipcodes.zipcode WHERE Zipcodes.state = MA";

        // parse query to extract attributes

    }

}
