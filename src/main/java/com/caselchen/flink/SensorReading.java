package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SensorReading implements Serializable {
    String id;
    Long timestamp;
    Double temperature;
}
