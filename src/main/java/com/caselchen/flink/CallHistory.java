package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class CallHistory implements Serializable {
    private String phoneNo;
    private Timestamp ts;
}
