//package com.caselchen.flink;
//
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.siddhi.SiddhiCEP;
//
//public class SiddhiCEPDemo {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
//
//        cep.registerExtension("custom:plus", CustomPlusFunctionExtension.class);
//
//        DataStream<Event> input1 = env.addSource(new RandomEventSource(5));
//        DataStream<Event> input2 = env.addSource(new RandomEventSource(5));
//
//        cep.registerStream("inputStream1", input1, "id", "name", "price", "timestamp");
//        cep.registerStream("inputStream2", input2, "id", "name", "price", "timestamp");
//
//        DataStream<Tuple5<Integer,String,Integer,String,Double>> output = cep
//                .from("inputStream1").union("inputStream2")
//                .cql(
//                        "from every s1 = inputStream1[id == 2] "
//                                + " -> s2 = inputStream2[id == 3] "
//                                + "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 , custom:plus(s1.price, s2.price) as price"
//                                + "insert into outputStream")
//                .returns("outputStream");
//
//        output.print();
//
//        env.execute();
//    }
//}
