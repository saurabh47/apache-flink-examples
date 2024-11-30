package org.apache.flink.examples.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

public class UnboundedSource implements SourceFunction<Tuple2<String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        Random random = new Random();
        while (running) {
            // Generate random events with timestamps
            String event = "Event" + random.nextInt(10000);
            long timestamp = System.currentTimeMillis() - random.nextInt(10000); // Simulate event times
            ctx.collectWithTimestamp(Tuple2.of(event, timestamp), timestamp);

            // Add delay to simulate real-time streaming
            Thread.sleep(500); // Emit events every 500 ms
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
