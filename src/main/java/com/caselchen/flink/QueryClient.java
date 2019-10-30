package com.caselchen.flink;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

public class QueryClient {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "Missing Flink Job ID ...\n"
                            + "Usage: ./QueryClient <jobID> [jobManagerHost] [jobManagerPort]");
        }

        final JobID jobId = JobID.fromHexString(args[0]);
        final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
        final int jobManagerPort = args.length > 1 ? Integer.parseInt(args[1]) : 9069;

        QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);

        /*
          状态描述符
          event_five_seconds_max_pv 状态名称，Server端此名称是通过UUID生成的，这里随意即可
          TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})) 状态中值的类型，应和Server端相同
         */
        ValueStateDescriptor<SensorReading> stateDescriptor =
                new ValueStateDescriptor<>(
                        "lastFiveSecondsMaxPV",
                        TypeInformation.of(new TypeHint<SensorReading>() {}));
        final String key = "click";

        /*
         轮询执行查询
         event_five_seconds_max_pv queryableStateName，应和Server端的queryableStateName相同
        */
        while (true) {
            CompletableFuture<ValueState<SensorReading>> completableFuture =
                    client.getKvState(
                            jobId,
                            "lastFiveSecondsMaxPV",
                            key,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            stateDescriptor);

            System.out.println(completableFuture.get().value());

            Thread.sleep(1000);
        }
    }
}


