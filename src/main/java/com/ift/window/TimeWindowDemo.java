package com.ift.window;

import com.ift.bean.WaterSensor;
import com.ift.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
/**
 * 触发器、移除器：现成的几个窗口都有默认的实现，一般不需要自定义
 * 以 时间类型的 滚动窗口 为例，分析原理：
 * 1.窗口什么时候触发 输出？
 *  时间进展 >= 窗口的最大时间戳（end - 1ms）
 * 2.窗口是怎么划分的？
 *  start = 向下取整，取窗口长度的整数倍
 *  end = start + 窗口长度
 * 3.窗口的生命周期？
 *  创建：属于本窗口的第一条数据来的时候，现new的，放入一个singleton单例的集合中
 *  销毁（关窗）： 时间进展 >= 窗口的最大时间戳（end - 1ms） + 允许迟到的时间（默认0）
 */

/**
 * 时间窗口
 * @author liufei
 */
public class TimeWindowDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);

        socketSource.map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
                    @Override
                    public long extract(WaterSensor element) {
                        return element.getTs() * 1000L;
                    }
                }))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        final long start = context.window().getStart();
                        final long end = context.window().getEnd();
                        final String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        final String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        System.out.println("窗口触发，key: " + s + "，数据量：" + elements.toString() + "，开始时间：" + startTime + "，结束时间：" + endTime);

                    }
                })
                .print();
        env.execute();
    }
}
