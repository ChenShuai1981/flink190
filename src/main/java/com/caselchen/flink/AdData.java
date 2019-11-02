package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AdData {
    private Integer aId;
    private Integer tId;
    private String clientId;
    private Integer actionType;
    private Long time;
}
