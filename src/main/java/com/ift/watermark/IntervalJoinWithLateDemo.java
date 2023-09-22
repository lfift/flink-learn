package com.ift.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 1.只支持事件时间
 * 2.指定上界、下界的偏移，负号代表时间往前，正号代表时间往后
 * 3.process中，只能处理join上的数据
 * 4.两条流关联后的watermark，以两条刘中最小的为准
 * 5.如果当前的事件时间 < 当前的watermark，就是迟到数据，主流的process不处理
 *  between后，可以指定将左流或右流迟到的数据放入侧输出流
 * @author liufei
 */
public class IntervalJoinWithLateDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .socketTextStream("192.168.3.24", 7777)
                .map(value -> {
                    final String[] values = value.split(",");
                    return Tuple2.of(values[0], Integer.valueOf(values[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1 * 1000L)));

        final SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .socketTextStream("192.168.3.24", 8888)
                .map(value -> {
                    final String[] values = value.split(",");
                    return Tuple3.of(values[0], Integer.valueOf(values[1]), Integer.valueOf(values[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1 * 1000L)));

        final OutputTag<Tuple2<String, Integer>> ds1Tag = new OutputTag<>("ds1-late", Types.TUPLE(Types.STRING, Types.INT));
        final OutputTag<Tuple3<String, Integer, Integer>> ds2Tag = new OutputTag<>("ds2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        //分别做keyBy，key其实就是关联条件
        final SingleOutputStreamOperator<String> process = ds1.keyBy(key -> key.f0)
                //调用intervalJoin
                .intervalJoin(ds2.keyBy(key -> key.f0))
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(ds1Tag)
                .sideOutputRightLateData(ds2Tag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<===>" + right);
                    }
                });

        process.print();
        process.getSideOutput(ds1Tag).printToErr("ds1-late");
        process.getSideOutput(ds2Tag).printToErr("ds2-late");
        env.execute();
    }
}
