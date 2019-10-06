package com.cloud.project.sqlUtils;

/**
 * Enum used to define the type of Aggregate Function to be used in Group By SQL Query
 */
public enum AggregateFunction {
    NONE(0), SUM(1), MAX(2), MIN(3), COUNT(4);
    private final int value;

    AggregateFunction(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
