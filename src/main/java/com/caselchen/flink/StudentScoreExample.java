package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;

public class StudentScoreExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        DataStream<String> textStream = env.fromElements("A,70,1", "A,80,2", "A,60,3", "A,90,4", "A,80,5", "A,70,6");
        DataStream<StudentScore> scoreStream = textStream.map((MapFunction<String, StudentScore>) value -> {
            String[] parts = value.split(",");
            return new StudentScore(parts[0], Float.valueOf(parts[1]), new Timestamp(Long.valueOf(parts[2])));
        });

        StreamTableEnvironment stblEnv = StreamTableEnvironment.create(env, setting);
        Table srcTbl = stblEnv.fromDataStream(scoreStream, "name, score, ts.rowtime");
        stblEnv.registerTable("score_table", srcTbl);

        Table resultTbl = stblEnv.sqlQuery("SELECT name, score, ts, ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts) rn FROM score_table");

        DataStream<Tuple2<Boolean, StudentScoreWithRn>> resultStream = stblEnv.toRetractStream(resultTbl, StudentScoreWithRn.class);

        resultStream.print();

        // 输出 color,count
        env.execute("StudentScoreExample");
    }

    public static class StudentScore {
        public String name;
        public float score;
        public Timestamp ts;
        public StudentScore() {}
        public StudentScore(String name, float score, Timestamp ts) {
            this.name = name;
            this.score = score;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "StudentScore{" +
                    "name=" + name +
                    ", score='" + score + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

    public static class StudentScoreWithRn {
        public String name;
        public float score;
        public Timestamp ts;
        public long rn;

        public StudentScoreWithRn() {}
        public StudentScoreWithRn(String name, float score, Timestamp ts, long rn) {
            this.name = name;
            this.score = score;
            this.ts = ts;
            this.rn = rn;
        }

        @Override
        public String toString() {
            return "StudentScoreWithRn{" +
                    "name=" + name +
                    ", score='" + score +
                    ", rn='" + rn +
                    ", ts=" + ts +
                    '}';
        }
    }

    public static class StudentScoreWithAvg {
        public String name;
        public float score;
        public Timestamp ts;
        public float avgScore;
        public StudentScoreWithAvg() {}
        public StudentScoreWithAvg(String name, float score, Timestamp ts, float avgScore) {
            this.name = name;
            this.score = score;
            this.ts = ts;
            this.avgScore = avgScore;
        }

        @Override
        public String toString() {
            return "StudentScoreWithAvg{" +
                    "name=" + name +
                    ", score='" + score +
                    ", ts=" + ts +
                    ", avgScore='" + avgScore +
                    '}';
        }
    }
}
