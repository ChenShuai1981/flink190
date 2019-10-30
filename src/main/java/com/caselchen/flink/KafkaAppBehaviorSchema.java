//package com.caselchen.flink;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.streaming.util.serialization.SerializationSchema;
//
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//
//import static org.apache.flink.util.Preconditions.checkNotNull;
//
//public class KafkaAppBehaviorSchema implements DeserializationSchema<AppBehavior>, SerializationSchema<AppBehavior> {
//
//    private static final long serialVersionUID = -5784600791822349178L;
//
//    private ObjectMapper objectMapper = new ObjectMapper();
//
//    // ------------------------------------------------------------------------
//    //  Kafka Serialization
//    // ------------------------------------------------------------------------
//
//    /** The charset to use to convert between strings and bytes.
//     * The field is transient because we serialize a different delegate object instead */
//    private transient Charset charset;
//
//    private String separator = ",";
//
//    /**
//     * Creates a new SimpleStringSchema that uses "UTF-8" as the encoding.
//     */
//    public KafkaAppBehaviorSchema() {
//        this(StandardCharsets.UTF_8);
//    }
//
//    /**
//     * Creates a new SimpleStringSchema that uses the given charset to convert between strings and bytes.
//     *
//     * @param charset The charset to use to convert between strings and bytes.
//     */
//    public KafkaAppBehaviorSchema(Charset charset) {
//        this.charset = checkNotNull(charset);
//    }
//
//    @Override
//    public AppBehavior deserialize(byte[] message) {
//        String rawData = new String(message, StandardCharsets.UTF_8);
//        String[] dataArr = rawData.split(separator);
//        return new Tuple4<>(dataArr[0], dataArr[1],
//                Integer.valueOf(dataArr[2]), Double.valueOf(dataArr[3]));
//    }
//
//    @Override
//    public boolean isEndOfStream(Tuple4<String, String, Integer, Double> nextElement) {
//        return false;
//    }
//
//    @Override
//    public byte[] serialize(AppBehavior element) {
//        return objectMapper.writeValueAsString(element).getBytes();
//    }
//
//    @Override
//    public TypeInformation<AppBehavior> getProducedType() {
//        return null;
//    }
//
//}