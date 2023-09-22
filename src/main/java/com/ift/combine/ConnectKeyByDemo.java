package com.ift.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liufei
 */
public class ConnectKeyByDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(2);

        final DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(Tuple2.of(1, "1"),
                Tuple2.of(1, "2"), Tuple2.of(2, "2"), Tuple2.of(3, "3"));

        final DataStreamSource<Tuple3<Integer, String, String>> source2 = env.fromElements(Tuple3.of(1, "1", "a1"), Tuple3.of(1, "1", "a2"), Tuple3.of(2, "2", "b"),
                Tuple3.of(3, "3", "c"));

        source1.connect(source2)
                .keyBy(t1 -> t1.f0, t2 -> t2.f0)
                .process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, String>() {
            private final Map<Integer, List<Tuple2<Integer, String>>> cache1 = new HashMap<>();
            private final Map<Integer, List<Tuple3<Integer, String, String>>> cache2 = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                List<Tuple2<Integer, String>> cacheValue = cache1.get(value.f0);
                if (cacheValue == null) {
                    cacheValue = new ArrayList<>();
                    cacheValue.add(value);
                    cache1.put(value.f0, cacheValue);
                } else {
                    cacheValue.add(value);
                }
                final List<Tuple3<Integer, String, String>> tuple3List = cache2.get(value.f0);
                if (tuple3List == null) {
                    return;
                }
                for (Tuple3<Integer, String, String> tuple3 : tuple3List) {
                    out.collect("tuple2: " + value + ",tuple3: " + tuple3);
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                List<Tuple3<Integer, String, String>> cacheValue = cache2.get(value.f0);
                if (cacheValue == null) {
                    cacheValue = new ArrayList<>();
                    cacheValue.add(value);
                    cache2.put(value.f0, cacheValue);
                } else {
                    cacheValue.add(value);
                }
                final List<Tuple2<Integer, String>> tuple2List = cache1.get(value.f0);
                if (tuple2List == null) {
                    return;
                }
                for (Tuple2<Integer, String> tuple2 : tuple2List) {
                    out.collect("tuple2: " + tuple2 + ",tuple3: " + value);
                }
            }
        }).print();

        env.execute();

    }
}
