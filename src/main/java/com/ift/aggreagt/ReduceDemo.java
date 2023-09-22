package com.ift.aggreagt;

import com.ift.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s1", 21L, 21),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3))
                .keyBy(WaterSensor::getId)
                /*
                    只能在keyBy之后调用
                    每个key的第一条数据来了之后不会计算，会暂存起来直接输出
                    reduce参数：
                        value1：之前的计算结果
                        value2：现在来的数据
                 */
                .reduce((value1, value2) -> {
                    System.out.println("value1:" + value1);
                    System.out.println("value2:" + value2);
                    return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                })
                .print();
        env.execute();

    }
}
