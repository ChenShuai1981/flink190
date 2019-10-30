package com.caselchen.flink;

import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.queryablestate.client.QueryableStateClient;

public class QueryClient2 {
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
        */
        ValueStateDescriptor<Tuple5<String, String, String, Long, Long>> valueStateDescriptor =
                new ValueStateDescriptor<>(
                        "lastFiveSecondsCountSumValueState",
                        Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG));

        final String key = "buy";

        /*
         轮询执行查询
         lastFiveSecondsCountSum queryableStateName，应和Server端的queryableStateName相同
        */
        while (true) {

            CompletableFuture<ValueState<Tuple5<String, String, String, Long, Long>>> completableFuture =
                    client.getKvState(
                            jobId,
                            "lastFiveSecondsCountSum",
                            key,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            valueStateDescriptor);

            Tuple5<String, String, String, Long, Long> value = completableFuture.get().value();
            System.out.println(
                    "Key: "
                            + value.f0
                            + " ,WindowStart: "
                            + value.f1
                            + " ,WindowEnd: "
                            + value.f2
                            + " ,Count: "
                            + value.f3
                            + " ,Sum: "
                            + value.f4);

            Thread.sleep(1000);
        }
    }
}

