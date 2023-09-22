package com.ift.transform;

import com.ift.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liufei
 */
public class FlatMapDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromElements(
                new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 2L, 2),
                        new WaterSensor("s3", 3L, 3))
                .flatMap((value, out) -> {
                    if (value.getId().equals("s1")) {
                        out.collect(value.getVc().toString());
                    } else if ("s2".equals(value.getId())) {
                        out.collect(value.getTs().toString());
                        out.collect(value.getVc().toString());
                    }
                }, Types.STRING)
                .print();
        env.execute();

    }
}
