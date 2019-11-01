package com.caselchen.flink;

import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class UpperStringFlatMapFunctionTest {

    @Test
    public void testUpperString() throws Exception {
        // instantiate your function
        UpperStringFlatMapFunction upper = new UpperStringFlatMapFunction();

        Collector<String> collector = mock(Collector.class);

        String message = "aaa";
        // call the methods that you have implemented
        upper.flatMap(message, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(message.toUpperCase());
    }
}
