package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * 定时输出
 */
public class BatchIntervalSinkJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(600000);

        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);
        DataStream<Object> outputStream = inputStream
                .transform("sink", TypeInformation.of(Object.class), new BatchIntervalSink(10, 10000))
                .setParallelism(1);

        outputStream.print();

        env.execute("BatchIntervalSinkJob");
    }

    static class BatchIntervalSink extends CommonSinkOperator<String> {
        public BatchIntervalSink(int batchSize, long batchInterval) {
            super(batchSize, batchInterval);
        }

        @Override
        public void saveRecords(List<String> datas) {
            datas.forEach(x -> System.out.println("saved " + x));
        }

    }
}
