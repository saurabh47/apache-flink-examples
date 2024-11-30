package org.apache.flink.examples.eventtime;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author Saurabh Gangamwar
 */
public class SystemTimeLagWatermarks<T> implements WatermarkGenerator<T> {

    private final long maxTimeLag = 50000; // 5 seconds

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }

    @Override
    public void onEvent(T event, long l, WatermarkOutput watermarkOutput) {

    }
}