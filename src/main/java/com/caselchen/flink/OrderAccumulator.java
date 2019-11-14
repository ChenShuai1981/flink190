package com.caselchen.flink;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class OrderAccumulator {
    private long siteId;
    private String siteName;
    private long subOrderSum;
    private long quantitySum;
    private long gmv;
    private Set<Long> orderIds = new HashSet<>();

    public void addOrderId(long orderId) {
        orderIds.add(orderId);
    }

    public void addSubOrderSum(long value) {
        subOrderSum += value;
    }

    public void addGmv(long value) {
        gmv += value;
    }

    public void addQuantitySum(long value) {
        quantitySum += value;
    }

    public void addOrderIds(Set<Long> values) {
        orderIds.addAll(values);
    }
}
