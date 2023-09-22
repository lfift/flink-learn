package com.ift.aggreagt;

import com.ift.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class SimpleAggreagtDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3))
                .keyBy(WaterSensor::getId)
                //非比较字段返回第一次的值
//                .max("vc")
                //非比较字段返回最新的值
                .maxBy("vc")
                .print();
        env.execute();

    }
}
