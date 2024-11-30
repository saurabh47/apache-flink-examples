package org.apache.flink.examples.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class IncrementalSourceWithIdlePeriod implements SourceFunction<Tuple2<String, Long>> {
    private volatile boolean running = true;
    private final long activeDurationMillis; // How long the source is active
    private final long idleDurationMillis;   // How long the source stays idle

    public IncrementalSourceWithIdlePeriod(long activeDurationMillis, long idleDurationMillis) {
        this.activeDurationMillis = activeDurationMillis;
        this.idleDurationMillis = idleDurationMillis;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        long currentTimestamp = System.currentTimeMillis();
        int counter = 0;
        long startTime = System.currentTimeMillis();

        while (running) {
            long elapsedTime = System.currentTimeMillis() - startTime;

            // Emit events during the active period
            if (elapsedTime < activeDurationMillis) {
                ctx.collectWithTimestamp(
                    Tuple2.of("Event" + counter, currentTimestamp), currentTimestamp
                );

                counter++;
                currentTimestamp += 1000; // Increment timestamp by 1 second
                Thread.sleep(1000); // Emit events every 1 second
            }
            // Simulate idle period after active period
            else if (elapsedTime < activeDurationMillis + idleDurationMillis) {
                Thread.sleep(1000); // Sleep without emitting events
            }
            // Reset the timer for the next cycle
            else {
                startTime = System.currentTimeMillis();
                counter = 0; // Reset counter for the next active period
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
