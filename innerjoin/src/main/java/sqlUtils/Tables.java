package sqlUtils;

public enum Tables {
    USERS(1), ZIPCODES(2), MOVIES(3), RATING(4);

    private final int value;

    Tables(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

