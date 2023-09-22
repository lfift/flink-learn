package com.ift.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合并流，被合并流类型必须一致，最终得到一个合并了所有流中元素的新流
 * 一次可以合并多个流
 * @author liufei
 */
public class UnionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        final DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);

        final DataStreamSource<Integer> stream2 = env.fromElements(4, 5, 6);

        final DataStreamSource<String> stream3 = env.fromElements("7", "8", "9");

        stream1.union(stream2).union(stream3.map(Integer::valueOf)).print();

        env.execute();
    }
}
