package com.caselchen.flink;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class GuavaDedup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Word> input = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        return new Word(value);
                    }
                });

        input.filter(new DedupeFilterFunction<>()).print();

        env.execute("GuavaDedup");
    }

    public static class DedupeFilterFunction<T extends Element> extends RichFilterFunction<T> implements CheckpointedFunction {
        private LoadingCache<String, Boolean> cache;
        private MapStateDescriptor<String, Boolean> mapStateDescriptor =
                new MapStateDescriptor<>("dedupCache", Types.STRING, Types.BOOLEAN);
        private MapState<String, Boolean> mapState;

        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            cache = CacheBuilder.newBuilder()
                    // 设置过期时间
                    .expireAfterWrite(5000, TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, Boolean>() {
                        @Override
                        public Boolean load(final String id) throws Exception {
                            return true;
                        }
                    });
        }

        public boolean filter(T value) throws Exception {
            String key = value.getId();
            boolean seen = cache.get(key);
            if (!seen) {
                cache.put(key, true);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            ConcurrentMap<String, Boolean> cacheMap = cache.asMap();
            for (Map.Entry<String, Boolean> entry : cacheMap.entrySet()) {
                mapState.put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            Iterator<Map.Entry<String, Boolean>> it = mapState.entries().iterator();
            while(it.hasNext()) {
                Map.Entry<String, Boolean> entry = it.next();
                cache.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Data
    public static abstract class Element {
        protected String id;
    }

    @Data
    public static class Word extends Element {
        public Word(String id) {
            this.id = id;
        }
    }
}
