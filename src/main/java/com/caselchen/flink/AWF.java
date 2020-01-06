package com.caselchen.flink;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AWF implements AllWindowFunction<ObjectNode, List<ObjectNode>, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(AWF.class);

    @Override
    public void apply(TimeWindow window, Iterable<ObjectNode> values, Collector<List<ObjectNode>> out) throws Exception {
        ArrayList<ObjectNode> model = Lists.newArrayList(values);
        if (model.size() > 0) {
            log.info("10 秒内收集到 employee 的数据条数是：" + model.size());
            out.collect(model);
            log.info("collect 执行完毕");
        }
    }
}