package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink中延时调用设计与实现
 * https://mp.weixin.qq.com/s/dUbT7qV6RZdjh7gLDMp4gQ
 *
 * 场景：
 * 服务器下线监控报警，服务器上下线都会发送一条消息，如果发送的是下线消息，
 * 在之后的10s内没有收到上线消息则循环发出警告，直到上线取消告警。
 *
 * 打开命令行 nc -l 9999 输入
 * 1,true
 * 2,true
 * 3,false
 * 4,true
 * 3,true
 * 2,false
 * 1,true
 * 4,true
 * 3,true
 * 2,true
 * 1,false
 *
 * 显示
 * 告警: 2 is offline, please restart
 * 告警: 1 is offline, please restart
 */
public class DetectDelayJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);
        DataStream<ServerMsg> serverMsgStream = inputStream.map((MapFunction<String, ServerMsg>) value -> {
            String[] parts = value.split(",");
            String serverId = parts[0];
            Boolean isOnline = Boolean.valueOf(parts[1]);
            return new ServerMsg(serverId, isOnline);
        });

        DataStream<String> outputStream = serverMsgStream
                .keyBy((KeySelector<ServerMsg, String>) value -> value.serverId)
                .process(new MonitorKeyedProcessFunction());

        outputStream.print();

        env.execute("DetectDelayJob");
    }

    @Data
    @AllArgsConstructor
    static class ServerMsg {
        private String serverId;
        private Boolean isOnline;
    }

    static class MonitorKeyedProcessFunction extends KeyedProcessFunction<String, ServerMsg, String> {

        private ValueState<Long> timeState = null;
        private ValueState<String> serverState = null;
        private long period = 10000L;

        @Override
        public void open(Configuration parameters) {
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<>("time-state", TypeInformation.of(Long.class)));
            serverState = getRuntimeContext().getState(new ValueStateDescriptor<>("server-state", TypeInformation.of(String.class)));
        }

        @Override
        public void processElement(ServerMsg value, Context ctx, Collector<String> out) throws Exception {
            if (!value.isOnline) {
                long monitorTime = ctx.timerService().currentProcessingTime() + period;
                timeState.update(monitorTime);
                serverState.update(value.serverId);
                ctx.timerService().registerProcessingTimeTimer(monitorTime);
            }

            if (value.isOnline && null != timeState.value()) {
                ctx.timerService().deleteProcessingTimeTimer(timeState.value());
                timeState.update(null);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp == timeState.value()) {
                long newMonitorTime = timestamp + period;
                timeState.update(newMonitorTime);
                ctx.timerService().registerProcessingTimeTimer(newMonitorTime);
                System.out.println("告警: " + serverState.value() + " is offline, please restart");
            }
        }
    }
}
