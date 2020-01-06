package com.caselchen.flink;

import io.siddhi.query.api.expression.Expression;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;

import java.time.Duration;

/**
 * https://blog.csdn.net/LS_ice/article/details/90711744
 *
 * Emit（Trigger）触发器
 * 配置方式指定Trigger：Flink1.9.0目前支持通过TableConifg配置earlyFireInterval、lateFireInterval毫秒数，来指定窗口结束之前、窗口结束之后的触发策略（默认是watermark超过窗口结束后触发一次），策略的解析在WindowEmitStrategy，在StreamExecGroupWindowAggregateRule就会创建和解析这个策略
 * SQL方式指定Trigger：Flink1.9.0代码中calcite部分已有SqlEmit相关的实现，后续可以支持SQL 语句（INSERT INTO）中配置EMIT触发器
 *
 * TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED
 * TABLE_EXEC_EMIT_EARLY_FIRE_DELAY
 * TABLE_EXEC_EMIT_LATE_FIRE_ENABLED
 * TABLE_EXEC_EMIT_LATE_FIRE_DELAY
 */
public class EmitTriggerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.fromElements("", "", "");

        env.execute();
    }
}
