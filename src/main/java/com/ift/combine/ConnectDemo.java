package com.ift.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 合并流，被合并流类型可以不一致，合并后只是形式上的统一，内部还是多个流
 * 一次只能合并两个流
 * @author liufei
 */
public class ConnectDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        final DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);

        final DataStreamSource<String> stream2 = env.fromElements("a", "b", "c");

        final ConnectedStreams<Integer, String> connectedStreams = stream1.connect(stream2);

        connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return String.valueOf(value);
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        }).print();

        env.execute();
    }
}
