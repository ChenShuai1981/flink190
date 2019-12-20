package com.caselchen.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MultiThreadConsumerClient implements Runnable {

    private Logger LOG = LoggerFactory.getLogger(MultiThreadConsumerClient.class);

    private LinkedBlockingQueue<String> bufferQueue;
    private CyclicBarrier barrier;

    private int batchSize = 5;
    private int timeout = 2000;
    private List<String> entities = new ArrayList<>();

    public MultiThreadConsumerClient(
            LinkedBlockingQueue<String> bufferQueue, CyclicBarrier barrier) {
        this.bufferQueue = bufferQueue;
        this.barrier = barrier;
        // 超时定时器
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        if (!entities.isEmpty()) {
                            System.out.println("timeout -> doSomething ...");
                            doSomething(entities);
                            entities = new ArrayList<>();
                        }
                    }
                },
                0,
                timeout,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        String entity;
        while (true){
            try {
                // 从 bufferQueue 的队首消费数据，并设置 timeout
                entity = bufferQueue.poll(50, TimeUnit.MILLISECONDS);
                // entity != null 表示 bufferQueue 有数据
                if(entity != null) {
                    if (entities.size() >= batchSize) {
                        // 执行 client 消费数据的逻辑
                        System.out.println("normal -> doSomething ...");
                        doSomething(entities);
                        entities = new ArrayList<>();
                    }
                    entities.add(entity);
                } else {
                    // entity == null 表示 bufferQueue 中已经没有数据了，
                    // 且 barrier wait 大于 0 表示当前正在执行 Checkpoint，
                    // client 需要执行 flush，保证 Checkpoint 之前的数据都消费完成
                    if ( barrier.getNumberWaiting() > 0 ) {
                        LOG.info("MultiThreadConsumerClient 执行 flush, " +
                                "当前 wait 的线程数：" + barrier.getNumberWaiting());
                        if (!entities.isEmpty()) {
                            flush();
                            entities = new ArrayList<>();
                        }
                        barrier.await();
                    }
                }
            } catch (InterruptedException| BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    // client 消费数据的逻辑
    private void doSomething(List<String> entities) {
        System.out.println("client doSomething ..." + entities.size());
    }

    // client 执行 flush 操作，防止丢数据
    // 如果client有攒批操作的话，内存中是会存在未处理的数据，需要flush处理
    private void flush() {
        // client.flush();
        System.out.println("client flushing ..." + entities.size());
    }

    private static String convertDataString(List<String> list) {
        String commaSeparated = list.stream().collect(Collectors.joining(","));
        return "[" + commaSeparated + "]";
    }

}