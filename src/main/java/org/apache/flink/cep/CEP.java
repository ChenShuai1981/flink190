package org.apache.flink.cep;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

public class CEP {
    public CEP() {
    }

    public static <T> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern) {
        return new PatternStream(input, pattern);
    }

    public static <T> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern, EventComparator<T> comparator) {
        PatternStream<T> stream = new PatternStream(input, pattern);
        return stream.withComparator(comparator);
    }

    public static <T> PatternStream<T> injectPattern(DataStream<T> input, InjectionPatternFunction function) {
        Pattern pattern = null;
        if (function.getPeriod() != 0) {
            // 启轮询线程
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
            ScheduledFuture<Pattern> scheduledFuture =
                    scheduledExecutorService.schedule(() -> getPattern(function), function.getPeriod(), TimeUnit.MILLISECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduledExecutorService.shutdown()));
            try {
                pattern = scheduledFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            pattern = getPattern(function);
        }
        return new PatternStream(input, pattern);
    }

    private static Pattern getPattern(InjectionPatternFunction function) {
        Pattern pattern = null;
        try {
            Map<String, Pattern> patternMap = function.inject();
            Collection<Pattern> patterns = patternMap.values();
            if (CollectionUtils.isNotEmpty(patterns)) {
                pattern = patterns.iterator().next();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load dynamic pattern", e);
        }
        return pattern;
    }
}
