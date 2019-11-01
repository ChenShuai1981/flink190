package com.caselchen.flink;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UpperStringFunctionTest {

    @Test
    public void testUpperString() {
        // instantiate your function
        UpperStringFunction upperStringFunction = new UpperStringFunction();
        String message = "aaa";
        // call the methods that you have implemented
        assertEquals(message.toUpperCase(), upperStringFunction.eval(message));
    }

}
