package com.ift.window;

import com.ift.bean.WaterSensor;
import com.ift.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 增量聚合和全窗口
 * @author liufei
 */
public class WindowAggregateAndProcessDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);

        socketSource.map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //增量聚合函数，来一条计算一条
                //窗口触发时增量聚合结果（只有一条）传递给全窗口函数
                //聚合结果经过全窗口函数的包装后再输出结果
                //可以解决增量聚合函数获取不到上下文信息问题
                .aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
                    /**
                     * 初始化累加器
                     *
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("初始化");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     *
                     * @param value       The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        return accumulator + value.getVc();
                    }

                    /**
                     * 窗口触发时获取结果
                     *
                     * @param accumulator The accumulator of the aggregation
                     * @return
                     */
                    @Override
                    public String getResult(Integer accumulator) {
                        return String.valueOf(accumulator);
                    }

                    /**
                     * 只有回话窗口才会被调用
                     *
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                }, new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        final long start = context.window().getStart();
                        final long end = context.window().getEnd();
                        final String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        final String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        System.out.println("窗口触发，key: " + s + "，数据量：" + elements.spliterator().estimateSize() + "数据：" + elements.iterator().next()+ "，开始时间：" + startTime + "，结束时间：" + endTime);
                    }
                })
                .print();
        env.execute();
    }
}
