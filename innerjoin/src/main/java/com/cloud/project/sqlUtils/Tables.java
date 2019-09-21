package com.cloud.project.sqlUtils;

public enum Tables {
    NONE(0), USERS(1), ZIPCODES(2), MOVIES(3), RATING(4);

    private final int value;

    Tables(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

