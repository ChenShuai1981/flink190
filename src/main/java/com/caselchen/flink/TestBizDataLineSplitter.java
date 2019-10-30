package com.caselchen.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * 原始消息参数处理类
 *
 */

public final class TestBizDataLineSplitter implements FlatMapFunction<String,
        Tuple4<String, String, Integer, Double>> {

    private static final long serialVersionUID = 1L;

    /**
     * 进行 map 阶段展开操作
     *
     * @param value 原始值: bizData: 2019-08-01 17:39:32,
     *              P0001,channel1,201908010116100001,100
     *
     *              dateTimeMin,
     *              productCode, channel,
     *              orderId, money
     *              [, totalCount, totalMoney]
     *
     * @param out 输出值, 用四元组保存
     *
     */
    @Override
    public void flatMap(String value, Collector<Tuple4<String, String,
            Integer, Double>> out) {
        String[] tokens = value.split(",");
        String time = tokens[0].substring(0, 16);
        String uniqDimKey = time + "," + tokens[1] + "," + tokens[2];
        // totalCount: 1, totalPremium: premium
        // todo: 写成 pojo

        out.collect(new Tuple4<>(tokens[3], uniqDimKey, 1, Double.valueOf(tokens[4])));
    }

}