package com.ift.window;

import com.ift.bean.WaterSensor;
import com.ift.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 全窗口
 * @author liufei
 */
public class WindowProcessDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);

        socketSource.map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
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
                        System.out.println("窗口触发，s: " + s + "，数据量：" + elements.spliterator().estimateSize() + "，开始时间：" + startTime + "，结束时间：" + endTime);

                    }
                })
                .print();
        env.execute();
    }
}
