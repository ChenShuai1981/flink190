package com.caselchen.flink;

import org.apache.flink.cep.pattern.Pattern;

import javax.script.Invocable;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class ScriptEngine {

    public static Pattern getPattern(String text, String name) throws ScriptException, NoSuchMethodException {
        ScriptEngineManager factory = new ScriptEngineManager();
        javax.script.ScriptEngine engine =  factory.getEngineByName("groovy");
//        System.out.println(engine.toString());
        assert engine != null;
        engine.eval(text);
        Pattern pattern = (Pattern)((Invocable)engine).invokeFunction(name);
        return pattern;
    }
}