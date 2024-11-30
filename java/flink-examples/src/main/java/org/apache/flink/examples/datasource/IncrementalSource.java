package org.apache.flink.examples.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class IncrementalSource implements SourceFunction<Tuple2<String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        long currentTimestamp = System.currentTimeMillis();
        int counter = 0;

        while (running) {
            // Emit an event with an incrementing counter and a timestamp
            ctx.collectWithTimestamp(
                Tuple2.of("Event" + counter, currentTimestamp), currentTimestamp
            );

            // Increment the counter and advance the timestamp by 1 second
            counter++;
            currentTimestamp += 1000; // Advance timestamp by 1 second

            // Simulate a delay to mimic real-time event emission
            Thread.sleep(500); // Emit events every 500 ms
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
