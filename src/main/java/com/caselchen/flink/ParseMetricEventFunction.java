package com.caselchen.flink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ParseMetricEventFunction implements FlatMapFunction<String, MetricEvent> {

    @Override
    public void flatMap(String value, Collector<MetricEvent> out) throws Exception {
        MetricEvent metricEvent = JSON.parseObject(value, MetricEvent.class);
        out.collect(metricEvent);
    }
}
