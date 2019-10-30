package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * {
 *     "search_time":1553650604,
 *     "code":200,
 *     "results":{
 *         "id":"449",
 *         "items":[
 *             {
 *                 "id":"47",
 *                 "name":"name47",
 *                 "title":"标题47",
 *                 "url":"https://www.google.com.hk/item-47",
 *                 "publish_time":1552884870,
 *                 "score":96.03
 *             },
 *             {
 *                 "id":"2",
 *                 "name":"name2",
 *                 "title":"标题2",
 *                 "url":"https://www.google.com.hk/item-2",
 *                 "publish_time":1552978902,
 *                 "score":16.06
 *             }
 *         ]
 *    }
 * }
 *
 **/
public class FlinkSqlJson {

    private static Logger LOG = LoggerFactory.getLogger(FlinkSqlJson.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Kafka kafka = new Kafka().version("universal")
                .topic("SearchResponse3")
                .startFromEarliest()
//                .startFromLatest()
                .property("bootstrap.servers", "localhost:9092")
                .property("group.id", "FlinkSqlJson2")
                .property("session.timeout.ms", "30000")
                .sinkPartitionerFixed();

        tableEnv.connect(kafka)
                .withFormat(new Json()
                        .failOnMissingField(false)
                        .deriveSchema())
                .withSchema(new Schema()
                        .field("search_time", Types.LONG())
                        .field("code", Types.INT())
                        .field("results", Types.ROW(
                                new String[]{"id", "items", "metas"},
                                new TypeInformation[]{
                                        Types.STRING(),
                                        ObjectArrayTypeInfo.getInfoFor(Row[].class,  //Array.newInstance(Row.class, 10).getClass(),
                                                Types.ROW(
                                                        new String[]{"id", "name", "title", "url", "publish_time", "score"},
                                                        new TypeInformation[]{Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG(),Types.FLOAT()}
                                                )),
                                        ObjectArrayTypeInfo.getInfoFor(Row[].class,  //Array.newInstance(Row.class, 10).getClass(),
                                                Types.ROW(
                                                        new String[]{"id", "name"},
                                                        new TypeInformation[]{Types.STRING(),Types.STRING()}
                                                ))})
                        )
                        .field("requests", Types.ROW(
                                new String[]{"id"},
                                new TypeInformation[]{
                                        Types.STRING()
                                }
                        ))).inAppendMode().registerTableSource("tb_json");

//item[1] item[10] 数组下标从1开始
        String sql4 = "select search_time, code, requests.id as request_id, results.id as result_id, results.items[1].name as item_1_name, results.items[2].id as item_2_id, results.metas[1].name as meta_1_name, results.metas[2].id as meta_2_id \n"
                + "from tb_json";

        Table table4 = tableEnv.sqlQuery(sql4);
        tableEnv.registerTable("tb_item_2", table4);
        LOG.info("------------------print {} schema------------------", "tb_item_2");
        table4.printSchema();

        tableEnv.toAppendStream(table4, Row.class).print();

//        tableEnv.registerTableSink("console4",
//                new String[]{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"},
//                new TypeInformation[]{
//                        Types.LONG(),Types.INT(),
//                        Types.STRING(),
//                        Types.STRING(),
//                        Types.STRING(),
//                        Types.STRING(),
//                        Types.STRING(),
//                        Types.STRING()
//                },
//                new PrintTableSink());
//
//        table4.insertInto("console4");

        // execute program
        env.execute("Flink Table Json Engine");
    }
}
