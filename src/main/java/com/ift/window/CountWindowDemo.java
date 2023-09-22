package com.ift.window;

import com.ift.bean.WaterSensor;
import com.ift.functions.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 计数窗口
 * @author liufei
 */
public class CountWindowDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);

        socketSource.map(new WaterSensorMapFunction())
                .keyBy(WaterSensor::getId)
//                .countWindow(5) //滚动窗口
                .countWindow(5, 2) //滑动窗口，长度为5步长为2（每经过一个步长都有一个窗口触发输出）
                .process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println("窗口触发，s: " + s + "，数据量：" + elements.toString());
                    }
                })
                .print();
        env.execute();
    }
}
