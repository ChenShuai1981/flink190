package com.caselchen.flink;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class MetricEvent {

    private String name;

    private long timestamp;

    private Map<String, Object> fields = new HashMap<>();

    private Map<String, String> tags = new HashMap<>();
}