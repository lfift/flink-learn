package com.ift.split;

import com.ift.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流
 *    /（侧输出流）
 * ===（source）---（主流）
 *    \（侧输出流）
 * @author liufei
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        final SingleOutputStreamOperator<WaterSensor> process = env.socketTextStream("192.168.3.24", 8888)
                .map((value -> {
                    final String[] values = value.split(",");
                    return new WaterSensor(values[0], Long.valueOf(values[1]), Integer.valueOf(values[2]));
                }))
                //底层算子，map，flatMap等算子都是通过该算子实现的
                //常见算子无法实现的可以通过该算子实现
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        if ("s1".equals(value.getId())) {
                            //放入侧输出流
                            final OutputTag<WaterSensor> tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
                            ctx.output(tag, value);
                        } else if ("s2".equals(value.getId())) {
                            //放入侧输出流
                            final OutputTag<WaterSensor> tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
                            ctx.output(tag, value);
                        } else {
                            //放入主流
                            out.collect(value);
                        }
                    }
                }, Types.POJO(WaterSensor.class));
        //打印主流
        process.print("main");
        //获取侧输出流并打印
        process.getSideOutput(new OutputTag<>("s1", Types.POJO(WaterSensor.class))).print("s1");
        process.getSideOutput(new OutputTag<>("s2", Types.POJO(WaterSensor.class))).print("s2");

        env.execute();
    }
}
