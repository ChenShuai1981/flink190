package com.caselchen.flink;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.queryablestate.client.QueryableStateClient;

public class TemperatureDashboard {

    private static String jobId = "4e7b8fd55b2eb6207ca089026f161556";
    private static DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("HH:mm:ss")
            .withLocale(Locale.PRC)
            .withZone( ZoneId.systemDefault() );

    public static void main(String[] args) throws Exception {
        String proxyHost = "10.15.232.5";
        int proxyPort = 9069;

        // how many sensors to query
        int numSensors = 5;
        // how often to query
        int refreshInterval = 10000;

        // configure client with host and port of queryable state proxy
        QueryableStateClient client = new QueryableStateClient(proxyHost, proxyPort);

        List<String> sensors = IntStream.range(0, numSensors)
                .mapToObj(i -> "sensor_" + (i+1))
                .collect(Collectors.toList());
        List<String> headers = new ArrayList<>();
        headers.add("WIN_START");
        headers.add("WIN_END");
        headers.addAll(sensors);

        String delimiter = "\t| ";
        String header = String.join(delimiter, headers);

        System.out.println(header);
        // loop forever
        while (true) {
            // send out async queries
            final CompletableFuture<ValueState<Tuple4<String, Long, Long, Double>>>[] futures = IntStream.range(0, numSensors)
                    .mapToObj(i -> queryState("sensor_" + (i + 1), client))
                    .toArray(CompletableFuture[]::new);

            // wait for results
            final Tuple4<String, Long, Long, Double>[] results = IntStream.range(0, numSensors)
                    .mapToObj(i -> {
                        Tuple4<String, Long, Long, Double> rs = null;
                        try {
                            rs = futures[i].get().value();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return rs;
                    })
                    .toArray(Tuple4[]::new);

            // print result
            Tuple4<String, Long, Long, Double> result = results[0];
            long winStart = result.f1;
            long winEnd = result.f2;
            String winStartString = formatDateTime(winStart);
            String winEndString = formatDateTime(winEnd);

            List<String> temps = Arrays.stream(results).map(t -> String.format("%4.3f" , t.f3)).collect(Collectors.toList());
            List<String> cells = new ArrayList<>();
            cells.add(winStartString);
            cells.add(winEndString);
            cells.addAll(temps);
            String line = String.join(delimiter, cells);
            System.out.println(line);

            // wait to send out next queries
            Thread.sleep(refreshInterval);
        }

//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            public void run() {
//                client.shutdownAndWait();
//            }
//        });
    }

    public static String formatDateTime(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return formatter.format(instant);
    }

    public static CompletableFuture<ValueState<Tuple4<String, Long, Long, Double>>> queryState(String key, QueryableStateClient client) {
        return client.getKvState(JobID.fromHexString(jobId),
                "maxTemperature", key, Types.STRING,
                new ValueStateDescriptor<Tuple4<String, Long, Long, Double>>("maxTempState", Types.TUPLE(Types.STRING, Types.LONG, Types.LONG, Types.DOUBLE)));
  }
}
