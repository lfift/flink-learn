package com.ift.functions;

import com.ift.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author liufei
 */
public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    private final String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor waterSensor) throws Exception {
        return this.id.equals(waterSensor.getId());
    }
}
