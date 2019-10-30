package com.caselchen.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

/**
 * @author : 恋晨
 * Date  : 2019/9/20 9:34 AM
 * 功能  :
 */
public class MyKeyedStateReaderFunction extends KeyedStateReaderFunction<String, Tuple2<String,Long>>{

    private ValueState<Long> wc;

    @Override
    public void open(Configuration configuration) throws Exception {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("WordCount", Types.LONG, 0L);

        this.wc = getRuntimeContext().getState(descriptor);

    }

    @Override
    public void readKey(String key, Context context, Collector<Tuple2<String,Long>> collector) throws Exception {



        collector.collect(new Tuple2<String, Long>(key, wc.value()));

    }
}