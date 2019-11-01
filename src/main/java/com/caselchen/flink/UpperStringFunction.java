package com.caselchen.flink;

import org.apache.flink.table.functions.ScalarFunction;

public class UpperStringFunction extends ScalarFunction {

    public String eval(String value) {
        if (value == null || value.trim().equals("")) {
            throw new IllegalArgumentException("value can not be blank");
        }
        return value.toUpperCase();
    }
}
