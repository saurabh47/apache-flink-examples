package org.apache.flink.examples;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.datasource.IncrementalSourceWithIdlePeriod;
import org.apache.flink.examples.eventtime.CustomBoundedOutOfOrdernessWatermarks;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/**
 * @author Saurabh Gangamwar
 */
public class CustomWatermarkExample {
    public static void main(String[] args) throws Exception {
        // Create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism (for simplicity in this example)
        env.setParallelism(1);

        // Add the source with an active period of 20 seconds and idle period of 50 seconds
        DataStream<Tuple2<String, Long>> input = env.addSource(new IncrementalSourceWithIdlePeriod(20000, 50000));

        // Assign timestamps and watermarks using the custom strategy
        DataStream<Tuple2<String, Long>> withTimestampsAndWatermarks = input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forGenerator(ctx ->
                        new CustomBoundedOutOfOrdernessWatermarks<>(
                                Duration.ofSeconds(10),      // Max out-of-orderness
                                Duration.ofSeconds(5)       // Idle source detection
                        )
                ).withTimestampAssigner((event, timestamp) -> event.f1) // Assign timestamps
        );

        // Process the data with a simple window operation
        withTimestampsAndWatermarks
                .keyBy(event -> event.f0) // Key by the event name (e.g., Event1, Event2)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5))) // Tumbling window of 5 seconds
                .sum(1) // Summing up the timestamps for demonstration
                .print(); // Print the result

        // Execute the Flink job
        env.execute("Custom Watermark Strategy Example");
    }
}
