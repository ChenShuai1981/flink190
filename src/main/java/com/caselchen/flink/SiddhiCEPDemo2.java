//package com.caselchen.flink;
//
//import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.siddhi.SiddhiCEP;
//import org.apache.flink.streaming.siddhi.SiddhiStream;
//
//import java.util.Map;
//
//public class SiddhiCEPDemo2 {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
////        DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");
////        DataStream<Event> input2 = env.addSource(new RandomEventSource(5), "input2");
////
////        DataStream<? extends Map> output = SiddhiCEP
////                .define("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp")
////                .union("inputStream2", input2.keyBy("id"), "id", "name", "price", "timestamp")
////                .cql(
////                        "from every s1 = inputStream1[id == 2] "
////                                + " -> s2 = inputStream2[id == 3] "
////                                + "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 "
////                                + "insert into outputStream"
////                )
////                .returnAsMap("JoinStream");
//
//        DataStream<Event> input = env.addSource(new RandomEventSource(5));
//        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
//        cep.registerExtension("custom:plus", CustomPlusFunctionExtension.class);
//
//        DataStream<Map<String, Object>> output = cep
//                .from("inputStream", input, "id", "name", "price", "timestamp")
//                .cql("from inputStream select timestamp, id, name, custom:plus(price,price) as doubled_price insert into  outputStream")
//                .returnAsMap("outputStream");
//
//        output.print();
//        env.execute();
//    }
//}
