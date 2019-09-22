package com.cloud.project.sqlUtils;

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
