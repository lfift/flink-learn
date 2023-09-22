package com.ift.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author liufei
 */
public class WatermarkIdlenessDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("192.168.3.24", 9999)
                .partitionCustom((key, numPartitions) -> Integer.parseInt(key) % numPartitions, key -> key)
                .map(Integer::valueOf)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element * 1000L))
                        //窗口延迟关闭时间
                        .withIdleness(Duration.ofSeconds(5L))
                )
                .keyBy(k -> k % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer s, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        final long start = context.window().getStart();
                        final long end = context.window().getEnd();
                        final String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        final String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        System.out.println("窗口触发，s: " + s + "，数据量：" + elements.spliterator().estimateSize() + "，开始时间：" + startTime + "，结束时间：" + endTime + "，数据=" + elements);
                    }
                })
                .print();

        env.execute();
    }
}
