package com.ift.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author liufei
 */
public class MyPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    /**
     * 乱序等待时间
     */
    private long delayTs;
    /**
     * 当前为止，最大事件时间
     */
    private long maxTs;

    public MyPeriodWatermarkGenerator(Long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;
    }

    /**
     * 来一条数据调用一次，用来提取最大的事件时间
     * @param event
     * @param eventTimestamp 提取到的数据的事件时间
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTs = Math.max(this.maxTs, eventTimestamp);
        System.out.println("调用了onEvent方法，获取的最大时间戳：" + this.maxTs);
    }

    /**
     * 周期性调用，生成Watermark
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(this.maxTs - this.delayTs - 1));
        System.out.println("调用了onPeriodicEmit方法：" + (this.maxTs - this.delayTs - 1));
    }
}
