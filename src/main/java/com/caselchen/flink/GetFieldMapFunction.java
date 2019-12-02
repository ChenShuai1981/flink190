package com.caselchen.flink;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorDouble;
import com.googlecode.aviator.runtime.type.AviatorObject;

import java.util.Map;

public class GetFieldMapFunction extends AbstractFunction {
    @Override
    public String getName() {
        return "getF";
    }

    @Override
    public AviatorDouble call(Map<String, Object> env, AviatorObject args1, AviatorObject args2) {
        Map<String, Object> map = (Map<String, Object>) FunctionUtils.getJavaObject(args1, env);
        String field = FunctionUtils.getStringValue(args2, env);
        return new AviatorDouble(Double.valueOf(String.valueOf(map.get(field))));
    }

}