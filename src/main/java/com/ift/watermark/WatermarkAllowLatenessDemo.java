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
 * 1.乱序与迟到的区别
 * 乱序：数据的顺序乱了，出现时间小的 比 时间大的晚来
 * 迟到：数据的时间戳 < 当前的Watermark
 * 2.乱序、迟到数据的处理
 * 2.1.watermark中指定 乱序等待时间
 * 2.2.如果开窗，设置窗口允许迟到
 *      推迟关窗时间，在关窗之前，迟到数据来了，还能被窗口计算，来一条迟到数据触发一次计算
 *      关闭窗口后迟到数据不会再计算
 * 2.3.关窗后迟到的数据放入侧输出流
 * 如果 watermark等待3s，窗口允许迟到2s，为什么不直接watermark允许等待5s或者窗口允许迟到5s
 * watermark等待时间不会设太大 ====》 影响计算延迟
 *      如果3s ==》 窗口第一次触发计算和输出， 13s的数据来 13-3=10s
 *      如果5s ==》 窗口第一次触发计算和输出， 15s的数据来 15-5=10s
 * 窗口允许迟到，是对大部分迟到数据的处理，尽量让结果准确
 *      如果只设置允许迟到5s，那么就会导致频繁重新输出
 * 设置经验：
 *  1.watermark等待时间，设置一个不算特别大的，一般是秒级，在乱序和延迟取舍
 *  2.设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到很久的数据，不管
 *  3.极端小部分迟到很久的数据，放到侧输出流。获取到之后可以做各种处理
 * @author liufei
 */
public class WatermarkAllowLatenessDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("192.168.3.24", 9999)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //乱序；等待3s
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        //提取事件时间
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
                //允许数据迟到时间，只能运行在事件时间上
                .allowedLateness(Time.seconds(2))
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
