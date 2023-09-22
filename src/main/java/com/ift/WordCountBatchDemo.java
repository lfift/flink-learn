package com.ift;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Hello world!
 *
 */
public class WordCountBatchDemo {
    public static void main( String[] args ) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final DataSource<String> fileSource = env.readTextFile("files/flink.txt");
        final FlatMapOperator<String, Tuple2<String, Integer>> operator = fileSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (in, out) -> {
            final String[] words = in.split(" ");
            for (String word : words) {
                final Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                out.collect(tuple2);
            }
        });
        final UnsortedGrouping<Tuple2<String, Integer>> grouping = operator.groupBy(0);
        final AggregateOperator<Tuple2<String, Integer>> sum = grouping.sum(1);
        sum.print();
    }
}
