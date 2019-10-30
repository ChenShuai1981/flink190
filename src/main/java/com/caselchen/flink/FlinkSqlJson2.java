package com.caselchen.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {
 * 	"search_time": 1567416604,
 * 	"code": 404,
 * 	"results": {
 * 		"id": "891",
 * 		"items": [{
 * 			"id": "1",
 * 			"name": "name1",
 * 			"title": "标题1",
 * 			"url": "https://www.google.com.hk/item-1",
 * 			"publish_time": 1566787103,
 * 			"score": 62.41
 *                }, {
 * 			"id": "81",
 * 			"name": "name81",
 * 			"title": "标题81",
 * 			"url": "https://www.google.com.hk/item-81",
 * 			"publish_time": 1566803309,
 * 			"score": 57.81
 *        }],
 * 		"metas": [{
 * 			"id": "1",
 * 			"name": "name1"
 *        }, {
 * 			"id": "81",
 * 			"name": "name81"
 *        }]
 *  },
 * 	"requests": {
 * 		"id": "370"
 * 	}
 * }
 */
public class FlinkSqlJson2 {

    private static Logger LOG = LoggerFactory.getLogger(FlinkSqlJson2.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.sqlUpdate("CREATE TABLE tb_json (\n" +
                "\tsearch_time BIGINT, \n" +
                "\tcode INT, \n" +
                "\tresults ROW<\n" +
                "\t\tid STRING, \n" +
                "\t\titems ARRAY<ROW<id STRING, name STRING, title STRING, url STRING, publish_time BIGINT, score FLOAT>>, \n" +
                "\t\tmetas ARRAY<ROW<id STRING, name STRING>>\n" +
                "\t>,\n" +
                "\trequests ROW<id STRING>\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',       \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'SearchResponse3',\n" +
                "  'update-mode' = 'append',\n" +
                "  'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "  'connector.properties.0.value' = 'localhost:2181',\n" +
                "  'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "  'connector.properties.1.value' = 'localhost:9092',\n" +
                "  'connector.properties.2.key' = 'group.id',\n" +
                "  'connector.properties.2.value' = 'testGroup',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")");

        String sql4 = "select search_time, code, requests.id as request_id, results.id as result_id, results.items[1].name as item_1_name, results.items[2].id as item_2_id, results.metas[1].name as meta_1_name, results.metas[2].id as meta_2_id from tb_json";

        Table table4 = tableEnv.sqlQuery(sql4);

        table4.printSchema();

        tableEnv.toAppendStream(table4, Row.class).print();

        // execute program
        env.execute("Flink Table Json Engine");
    }
}
