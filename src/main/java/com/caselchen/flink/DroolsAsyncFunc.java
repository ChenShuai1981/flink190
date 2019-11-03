package com.caselchen.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.kie.api.runtime.KieSession;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

public class DroolsAsyncFunc extends RichAsyncFunction<Person, String> {

    private String droolsFilePath;
    private KieSession ks;

    public DroolsAsyncFunc(String droolsFilePath) {
        super();
        this.droolsFilePath = droolsFilePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String content = String.join("\n", Files.readAllLines(Paths.get(droolsFilePath)));
        ks = DroolsSessionFactory.createDroolsSessionFromDrl(content);
    }

    @Override
    public void asyncInvoke(Person person, ResultFuture<String> resultFuture) throws Exception {
        ks.insert(person);
        ks.fireAllRules();
        resultFuture.complete(Collections.singleton(person.getResult()));
    }
}
