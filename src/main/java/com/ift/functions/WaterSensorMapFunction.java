package com.ift.functions;

import com.ift.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author liufei
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        final String[] values = value.split(",");
        return new WaterSensor(values[0], Long.valueOf(values[1]), Integer.valueOf(values[2]));
    }
}
