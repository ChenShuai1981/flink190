package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static org.hamcrest.Matchers.*;

public class MyDistinctFlatMapFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Long> testHarness;
    private MyDistinctFlatMapFunction myDistinctFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {
        //instantiate user-defined function
        myDistinctFlatMapFunction = new MyDistinctFlatMapFunction();

        // wrap user defined function into a the corresponding operator
        testHarness = new KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Long>(
                new StreamFlatMap<>(myDistinctFlatMapFunction),
                new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return 100;
                    }
                }, Types.INT);

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulFlatMapFunction() throws Exception {
        //push (timestamped) elements into the operator (and hence user defined function)
        for (int i=1; i<=10; i++) {
            testHarness.processElement(i, i);
        }
        testHarness.processElement(9, 11);
        testHarness.processElement(10, 12);

        Queue queue = testHarness.getOutput();
        Assert.assertEquals(12, queue.size());
        Iterator it = queue.iterator();
        List<Long> valueList = new ArrayList();
        Long lastValue = null;
        while(it.hasNext()) {
            StreamRecord record = (StreamRecord) it.next();
            lastValue = (Long) record.getValue();
            valueList.add(lastValue);
        }
        Assert.assertEquals(10, lastValue.longValue());
        Assert.assertThat(valueList, containsInRelativeOrder(1L,2L,3L,4L,5L,6L,7L,8L,9L,10L,10L,10L));
    }
}
