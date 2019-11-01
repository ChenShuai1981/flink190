package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static org.hamcrest.Matchers.containsInRelativeOrder;

public class AlarmFunctionTest {
    private OneInputStreamOperatorTestHarness<Integer, Integer> testHarness;
    private AlarmFunction alarmFunction;

    private OutputTag<Integer> alarmTag = new OutputTag<Integer>("alarm"){};

    @Before
    public void setupTestHarness() throws Exception {
        //instantiate user-defined function
        alarmFunction = new AlarmFunction();
        alarmFunction.setAlarmTag(alarmTag);

        // wrap user defined function into a the corresponding operator
        testHarness = new OneInputStreamOperatorTestHarness<Integer, Integer>(new ProcessOperator<>(alarmFunction));

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingProcessElement() throws Exception {
        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement(17, 100L);
        testHarness.processElement(18, 101L);
        testHarness.processElement(19, 102L);
        testHarness.processElement(16, 103L);

        //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
        Queue alarmQueue = testHarness.getSideOutput(alarmTag);
        List<Integer> alarmValues = new ArrayList<>();
        Iterator alarmIt = alarmQueue.iterator();
        while(alarmIt.hasNext()) {
            StreamRecord record = (StreamRecord) alarmIt.next();
            alarmValues.add((Integer)record.getValue());
        }
        Assert.assertThat(alarmValues, containsInRelativeOrder(17, 16));

        Queue outputQueue = testHarness.getOutput();
        List<Integer> values = new ArrayList<>();
        Iterator it = outputQueue.iterator();
        while(it.hasNext()) {
            StreamRecord record = (StreamRecord) it.next();
            values.add((Integer)record.getValue());
        }
        Assert.assertThat(values, containsInRelativeOrder(17, 18, 19, 16));
    }
}
