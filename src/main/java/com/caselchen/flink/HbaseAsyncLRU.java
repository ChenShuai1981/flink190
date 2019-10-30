package com.caselchen.flink;

import java.util.*;
import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import com.stumbleupon.async.Callback;

@Slf4j
public class HbaseAsyncLRU extends RichAsyncFunction<String, String> {

    private String zk;
    private String tableName;
    private long maxSize;
    private long ttl;
    private int batchSize;
    private long timerDelay;
    private long timerPeriod;

    private HBaseClient hBaseClient;
    private Cache<String, String> cache;
    private List<InputResultFuture> inputResultFutures = new ArrayList<>();
    private Timer timer;
    private TimerTask timerTask;

    /**
     *
     * @param zk hbase zk地址
     * @param tableName hbase表名
     * @param maxSize 缓存cache大小
     * @param ttl 缓存cache元素ttl
     * @param batchSize 攒批大小
     */
    public HbaseAsyncLRU(String zk, String tableName, long maxSize, long ttl, int batchSize, long timerDelay, long timerPeriod) {
        this.zk = zk;
        this.tableName = tableName;
        this.maxSize = maxSize;
        this.ttl = ttl;
        this.batchSize = batchSize;
        this.timerDelay = timerDelay;
        this.timerPeriod = timerPeriod;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("===> open()... <===");
        hBaseClient = new HBaseClient(zk);
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttl, TimeUnit.SECONDS)
                .build();
        timer = new Timer();
    }

    @Override
    public synchronized void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        System.out.println("===> asyncInvoke()... <===");
        String value = cache.getIfPresent(input);
        if (value != null) {
            log.debug(String.format("get cache: %s -> %s", input, value));
            log.debug("complete >> " + input);
            resultFuture.complete(Collections.singleton(fillData(input, value)));
        } else {
            inputResultFutures.add(new InputResultFuture(input, resultFuture));
            if (inputResultFutures.size() > batchSize){
                doInvoke();
            } else {
                if (timerTask != null) {
                    timerTask.cancel();
                }
                timerTask = new TimerTask() {
                    public void run() {
                        doInvoke();
                    }
                };
                timer.schedule(timerTask, timerDelay , timerPeriod);
            }
        }
    }

    private synchronized void doInvoke() {
        List<GetRequest> batchGets = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(inputResultFutures)) {
            for (InputResultFuture inputResultFuture : inputResultFutures) {
                String input = inputResultFuture.getInput();
                GetRequest get = new GetRequest(tableName, input);
                batchGets.add(get);
            }
        }

        // hbase batch get request
        if (CollectionUtils.isNotEmpty(batchGets)) {
            hBaseClient.get(batchGets).addCallbacks(getResultOrExceptions -> {
                for (int i = 0; i < getResultOrExceptions.size(); i++) {
                    GetResultOrException getResultOrException = getResultOrExceptions.get(i);
                    InputResultFuture inputResultFuture = inputResultFutures.get(i);
                    ResultFuture resultFuture = inputResultFuture.getResultFuture();
                    Exception exception = getResultOrException.getException();
                    if (exception != null) {
                        // 批中某个元素get请求结果异常
                        resultFuture.completeExceptionally(exception);
                    } else {
                        // 批中某个元素get请求结果正常
                        ArrayList<KeyValue> keyValues = getResultOrException.getCells();
                        Tuple2<String, String> tuple2 = parseRs(keyValues);
                        String key = tuple2.f0;
                        String value = tuple2.f1;
                        // 更新cache
                        cache.put(key, value);
                        log.debug(String.format("put cache: %s -> %s", key, value));
                        log.debug("complete >> " + key);
                        resultFuture.complete(Collections.singleton(fillData(key, value)));
                    }
                }
                // 清空攒批容器
                inputResultFutures.clear();
                return "";
            }, (Callback<String, Exception>) e -> {
                // 处理批异常
                for (InputResultFuture inputResultFuture : inputResultFutures) {
                    String input = inputResultFuture.getInput();
                    ResultFuture resultFuture = inputResultFuture.getResultFuture();
                    log.debug("completeExceptionally >> " + input);
                    resultFuture.completeExceptionally(e);
                }
                // 清空攒批容器
                inputResultFutures.clear();
                return "";
            });
        }
    }

    // 针对每一行row解析出数据: 列簇=cf，列名=name
    private Tuple2<String, String> parseRs(ArrayList<KeyValue> keyValues) {
        String key = "";
        String value = "";
        for (KeyValue keyValue : keyValues) {
            String family = Bytes.toString(keyValue.family());
            String qualifier = Bytes.toString(keyValue.qualifier());
            if (family.equals("cf") && qualifier.equals("name")) {
                key = Bytes.toString(keyValue.key());
                value = Bytes.toString(keyValue.value());
                break;
            }
        }
        return new Tuple2<>(key, value);
    }

    // 填充数据
    private String fillData(String input, String value) {
        return input + " | " + value;
    }

    @Data
    @AllArgsConstructor
    static class InputResultFuture {
        private String input;
        private ResultFuture resultFuture;
    }

}
