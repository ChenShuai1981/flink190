package com.caselchen.flink;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

import java.util.Map;

public class GetTagMapFunction extends AbstractFunction {
    @Override
    public String getName() {
        return "getT";
    }
    @Override
    public AviatorString call(Map<String, Object> env, AviatorObject args1, AviatorObject args2) {
        Map<String, String> map = (Map<String, String>) FunctionUtils.getJavaObject(args1, env);
        String field = FunctionUtils.getStringValue(args2, env);
        return new AviatorString(map.get(field));
    }
}