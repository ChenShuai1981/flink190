package com.caselchen.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class StudentScoreExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 输入 name,color
//        DataStream<String> textStream = env.socketTextStream("localhost", 9999);

        DataStream<String> textStream = env.fromElements("A,70,1", "A,80,2", "A,60,3", "A,90,4", "A,80,5", "A,70,6");
        DataStream<StudentScore> scoreStream = textStream.map((MapFunction<String, StudentScore>) value -> {
            String[] parts = value.split(",");
            return new StudentScore(parts[0], Float.valueOf(parts[1]), Long.valueOf(parts[2]));
        });

        StreamTableEnvironment stblEnv = StreamTableEnvironment.create(env);
        Table srcTbl = stblEnv.fromDataStream(scoreStream, "name, score, ts");
        stblEnv.registerTable("score_table", srcTbl);

//        Table resultTbl = stblEnv.sqlQuery("select name, score, ts, ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts) as idx FROM score_table");

        Table resultTbl = stblEnv.sqlQuery("SELECT name ,\n" +
                "       score ,\n" +
                "       ts ,\n" +
                "       avg(score_between) avgScore\n" +
                "FROM\n" +
                "  (SELECT t1.*,\n" +
                "          t2.score score_between,\n" +
                "          ROW_NUMBER() OVER(PARTITION BY t1.name, t1.rnt\n" +
                "                            ORDER BY t2.score DESC) rrid\n" +
                "   FROM\n" +
                "     (SELECT name,\n" +
                "             score,\n" +
                "             ts,\n" +
                "             ROW_NUMBER() OVER(PARTITION BY name\n" +
                "                               ORDER BY ts) AS rnt\n" +
                "      FROM score_table) t1\n" +
                "   LEFT JOIN\n" +
                "     (SELECT name,\n" +
                "             score,\n" +
                "             ts,\n" +
                "             ROW_NUMBER() OVER(PARTITION BY name\n" +
                "                               ORDER BY ts) AS rnt\n" +
                "      FROM score_table) t2 ON t1.name = t2.name\n" +
                "   AND t1.rnt - t2.rnt BETWEEN 0 AND 2)a\n" +
                "WHERE a.rrid <= 2\n" +
                "GROUP BY name ,\n" +
                "         score ,\n" +
                "         ts\n" +
                "ORDER BY name,\n" +
                "         ts");

        DataStream<Tuple2<Boolean, StudentScoreWithAvg>> resultStream = stblEnv.toRetractStream(resultTbl, StudentScoreWithAvg.class);

        resultStream.print();

        // 输出 color,count
        env.execute("StudentScoreExample");
    }

    public static class StudentScore {
        public String name;
        public float score;
        public long ts;
        public StudentScore() {}
        public StudentScore(String name, float score, long ts) {
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

    public static class StudentScoreWithIdx {
        public String name;
        public float score;
        public long ts;
        public long idx;

        public StudentScoreWithIdx() {}
        public StudentScoreWithIdx(String name, float score, long ts, long idx) {
            this.name = name;
            this.score = score;
            this.ts = ts;
            this.idx = idx;
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

    public static class StudentScoreWithAvg {
        public String name;
        public float score;
        public long ts;
        public float avgScore;
        public StudentScoreWithAvg() {}
        public StudentScoreWithAvg(String name, float score, long ts, float avgScore) {
            this.name = name;
            this.score = score;
            this.ts = ts;
            this.avgScore = avgScore;
        }
    }
}
