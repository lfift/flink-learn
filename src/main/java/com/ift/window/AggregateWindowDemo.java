package com.ift.window;

import com.ift.bean.WaterSensor;
import com.ift.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 增量聚合，输入类型 中间类型 输出类型可以不一致
 * @author liufei
 */
public class AggregateWindowDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);

        socketSource.map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 属于本窗口的第一条数据到来，创建窗口并初始化累加器（调用createAccumulator方法）
                //来一条数据计算一条（调用add方法）
                //窗口触发时获取结果（调用getResult）
                .aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("初始化");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     * @param value The value to add
                     * @param accumulator The accumulator to add the value to
                     * @return
                     */
                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("聚合");
                        return accumulator + value.getVc();
                    }

                    /**
                     * 窗口触发时获取结果
                     * @param accumulator The accumulator of the aggregation
                     * @return
                     */
                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("获取结果");
                        return String.valueOf(accumulator);
                    }

                    /**
                     * 只有回话窗口才会被调用
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                })
                .print();
        env.execute();
    }
}
