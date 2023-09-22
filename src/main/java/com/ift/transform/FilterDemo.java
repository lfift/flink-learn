package com.ift.transform;

import com.ift.bean.WaterSensor;
import com.ift.functions.FilterFunctionImpl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class FilterDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3))
                .filter(new FilterFunctionImpl("s1"))
                .print();
        env.execute();

    }
}
