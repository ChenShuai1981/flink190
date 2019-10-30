package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * kafka 自定义序列化器
 */
public class KafkaTuple4StringSchema  implements DeserializationSchema<Tuple4<String, String, Integer, Double>>, SerializationSchema<Tuple4<String, String, Integer, Double>> {

    private static final long serialVersionUID = -5784600791822349178L;

    // ------------------------------------------------------------------------
    //  Kafka Serialization
    // ------------------------------------------------------------------------

    /** The charset to use to convert between strings and bytes.
     * The field is transient because we serialize a different delegate object instead */
    private transient Charset charset;

    private String separator = ",";

    /**
     * Creates a new SimpleStringSchema that uses "UTF-8" as the encoding.
     */
    public KafkaTuple4StringSchema() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Creates a new SimpleStringSchema that uses the given charset to convert between strings and bytes.
     *
     * @param charset The charset to use to convert between strings and bytes.
     */
    public KafkaTuple4StringSchema(Charset charset) {
        this.charset = checkNotNull(charset);
    }

    @Override
    public Tuple4<String, String, Integer, Double> deserialize(byte[] message) {
        String rawData = new String(message, StandardCharsets.UTF_8);
        String[] dataArr = rawData.split(separator);
        return new Tuple4<>(dataArr[0], dataArr[1],
                Integer.valueOf(dataArr[2]), Double.valueOf(dataArr[3]));
    }

    @Override
    public boolean isEndOfStream(Tuple4<String, String, Integer, Double> nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Tuple4<String, String, Integer, Double> element) {
        return (element.f0 + separator +
                element.f1 + separator +
                element.f2 + separator +
                element.f3).getBytes();
    }

    @Override
    public TypeInformation<Tuple4<String, String, Integer, Double>> getProducedType() {
        return null;
    }

}