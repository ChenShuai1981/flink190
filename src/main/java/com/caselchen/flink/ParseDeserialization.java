package com.caselchen.flink;

import com.alibaba.fastjson.JSON;

import java.io.IOException;

public class ParseDeserialization extends AbsDeserialization<RawData> {
    @Override
    public RawData deserialize(byte[] message) throws IOException {
        try {
            String msg = new String(message);
            RawData rawData = JSON.parseObject(msg, RawData.class);
            normalDataNum.inc();
            return rawData;
        } catch (Exception e) {
            dirtyDataNum.inc();
            return null;
        }
    }
}
