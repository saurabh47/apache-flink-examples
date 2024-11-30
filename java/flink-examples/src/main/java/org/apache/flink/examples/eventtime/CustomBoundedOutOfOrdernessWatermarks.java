package org.apache.flink.examples.eventtime;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * Custom watermark strategy for generating watermarks based an event-time.
 * This implementation handles out-of-order events by setting a maximum allowed delay (outOfOrderness)
 * and handles idle sources by configuring a maximum idleness period (sourceIdleness).
 *
 * <p> This watermark generator:
 * <ul>
 *   <li>Tracks the highest observed event timestamp and applies a delay to manage out-of-order events.</li>
 *   <li>Emits watermarks periodically based on the maximum observed timestamp, accounting for the out-of-order allowance.</li>
 *   <li>Advances watermarks even when sources are idle, based on the configured idleness threshold.</li>
 * </ul>
 *
 * @param <T> The type of the input events.
 * @see WatermarkGenerator
 * @see WatermarkOutput
 *
 * @author Saurabh Gangamwar
 */
public class CustomBoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {
    private long maxTimestamp;
    private final long outOfOrdernessMillis;
    private long lastMaxTimestampCalculatedAt;
    private long sourceMaxIdlenessMillis;

    private long lastEmittedWatermark = Long.MIN_VALUE;

    public CustomBoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness, Duration sourceIdleness) {
        Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        Preconditions.checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
        Preconditions.checkNotNull(sourceIdleness, "sourceIdleness");
        Preconditions.checkArgument(!sourceIdleness.isNegative(), "sourceIdleness cannot be negative");
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        this.sourceMaxIdlenessMillis = sourceIdleness.toMillis();
        this.maxTimestamp = Long.MIN_VALUE + this.outOfOrdernessMillis + 1L;
    }

    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        if(eventTimestamp > maxTimestamp) {
            maxTimestamp = eventTimestamp;
        }
        lastMaxTimestampCalculatedAt = System.currentTimeMillis();
    }

    public void onPeriodicEmit(WatermarkOutput output) {
        long potentialWM = maxTimestamp - outOfOrdernessMillis - 1L;


        if(potentialWM > lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        } else if ((System.currentTimeMillis() - lastMaxTimestampCalculatedAt) > sourceMaxIdlenessMillis) {
            // Truncate maxTimestamp to the nearest 10-second interval and trails the watermark by outOfOrdernessMillis + 1 ms to close the window
            potentialWM = ((maxTimestamp / 10000) * 10000) + outOfOrdernessMillis + 1L;
            lastEmittedWatermark = Math.max(lastEmittedWatermark,  potentialWM);
            System.out.println("Increment watermark:"+ lastEmittedWatermark);
        }
        output.emitWatermark(new Watermark(this.lastEmittedWatermark));
    }
}