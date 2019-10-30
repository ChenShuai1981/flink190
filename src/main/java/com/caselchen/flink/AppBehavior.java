package com.caselchen.flink;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class AppBehavior {
    private String eventName;
    private String terminal;
    private String fpid;
    private String eventTime;
    private String latitude;
    private String longitude;
    private String messages;
    private String applist;
    private String applistnew;
    private String calllog;
}
