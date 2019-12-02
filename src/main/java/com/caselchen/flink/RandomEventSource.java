package com.caselchen.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomEventSource implements SourceFunction<Event> {
    private String name;
    private final int count;
    private final Random random;
    private final long initialTimestamp;

    private volatile boolean isRunning = true;
    private volatile AtomicInteger number = new AtomicInteger(0);
    private volatile long closeDelayTimestamp = 1000;

    public RandomEventSource(int count, long initialTimestamp) {
        this.name = null;
        this.count = count;
        this.random = new Random();
        this.initialTimestamp = initialTimestamp;
    }

    public RandomEventSource() {
        this(Integer.MAX_VALUE, System.currentTimeMillis());
    }

    public RandomEventSource(int count) {
        this(count, System.currentTimeMillis());
    }

    public RandomEventSource closeDelay(long delayTimestamp) {
        this.closeDelayTimestamp = delayTimestamp;
        return this;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (isRunning) {
            Thread.sleep(200);
            long timestamp = initialTimestamp + 1000 * number.get();
            ctx.collectWithTimestamp(Event.of(number.get() % 50,
                    (this.name == null ? "test_event" : this.name), random.nextDouble(), timestamp), timestamp);
            if (number.incrementAndGet() >= this.count) {
                cancel();
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
        try {
            Thread.sleep(closeDelayTimestamp);
        } catch (InterruptedException e) {
            // ignored
        }
    }

    public RandomEventSource setName(String name) {
        this.name = name;
        return this;
    }
}
