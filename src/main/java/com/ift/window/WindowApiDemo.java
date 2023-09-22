package com.ift.window;

import com.ift.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class WindowApiDemo {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<String> socketSource = env.socketTextStream("192.168.3.24", 9999);
        final SingleOutputStreamOperator<WaterSensor> streamOperator = socketSource.map(value -> {
            final String[] values = value.split(",");
            return new WaterSensor(values[0], Long.valueOf(values[1]), Integer.valueOf(values[2]));
        });
        final KeyedStream<WaterSensor, String> keyedStream = streamOperator.keyBy(WaterSensor::getId);

        //1.指定窗口，指定使用哪一种窗口 时间窗口、计数窗口；滚动 滑动 会话
        //1.1.没有keyBy，所有数据进入同一个子任务，并行度只能为1
//        socketSource.windowAll()
        //1.2.有keyBy，每一个key上都有一个窗口进行独立的统计运算

        //基于时间的窗口
//        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10))); //滚动窗口，窗口长度10s
//        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(3))); //滑动窗口，窗口长度10s，滑动步长3s
//        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))); //会话窗口，会话间隔10s，超过10s没有数据来就创建一个新的窗口，之前的数据属于前一个窗口

        //基于数量的窗口
//        keyedStream.countWindow(10); //滚动窗口，窗口内元素数量10
//        keyedStream.countWindow(10, 3); //滑动窗口，窗口内元素10，滑动步长3
//        keyedStream.window(GlobalWindows.create()); //全局窗口，计数窗口底层就是使用的这个，需要自定义的时候用

        //2.指定窗口内数据的计算函数
//        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        //增量聚合：来一条计算一条，窗口触发的时候输出结果
//                .reduce()
//                .aggregate()
        //全窗口函数：数据来了不计算，窗口触发的时候计算并输出结果
//                .apply()
//                .process()
    }
}
