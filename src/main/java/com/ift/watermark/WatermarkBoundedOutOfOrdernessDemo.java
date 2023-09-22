package com.ift.watermark;

import com.ift.bean.WaterSensor;
import com.ift.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
public class WatermarkBoundedOutOfOrdernessDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("192.168.3.24", 9999)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                System.out.println("element=" + element + ",recordTimestamp=" + recordTimestamp);
                                return element.getTs() * 1000L;
                            }
                        })
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * 窗口触发时才计算
                     * @param s The key for which this window is evaluated.
                     * @param context The context in which the window is being evaluated.
                     * @param elements The elements in the window being evaluated.
                     * @param out A collector for emitting elements.
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
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
