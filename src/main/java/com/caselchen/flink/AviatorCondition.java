package com.caselchen.flink;

import com.googlecode.aviator.AviatorEvaluator;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AviatorCondition extends SimpleCondition<MetricEvent> implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(AviatorCondition.class);

    private String script;

    static {
        AviatorEvaluator.addFunction(new GetFieldMapFunction());
        AviatorEvaluator.addFunction(new GetTagMapFunction());
        AviatorEvaluator.addFunction(new MinFunction());
    }

    public AviatorCondition(String script) {
        this.script = script;
    }

    @Override
    public boolean filter(MetricEvent event) throws Exception {
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("event", event);
        env.put("fields", event.getFields());
        env.put("tags", event.getTags());
        Boolean result = false;
        try {
            result = (Boolean) AviatorEvaluator.execute(script, env);
        } catch (Exception e) {
            logger.error("execute script with event error,script: {}, event: {}, error: {}", script, event, e);
        }
        return result;
    }

}