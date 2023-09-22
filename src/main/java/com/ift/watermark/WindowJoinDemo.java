package com.ift.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 基于时间窗口的双流联结
 * @author liufei
 */
public class WindowJoinDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 11),
                        Tuple2.of("b", 1),
                        Tuple2.of("b", 13))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1 * 1000L)));

        final SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 11),
                        Tuple3.of("b", 1, 1),
                        Tuple3.of("b", 13, 13),
                        Tuple3.of("c", 15, 15)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1 * 1000L)));
        /**
         * 1.落在同一个时间窗口内才能匹配
         * 2.根据keyBy的key来关联
         * 3.只能拿到匹配上的数据，类似有固定时间范围的inner join
         */
        ds1.join(ds2)
                //ds1的keyBy
                .where(key -> key.f0)
                //ds2的keyBy
                .equalTo(key -> key.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((first, second) -> first + "<===>" + second)
                .print();
        env.execute();
    }
}
