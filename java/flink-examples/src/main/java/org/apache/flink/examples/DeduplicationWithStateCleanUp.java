package org.apache.flink.examples;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.examples.entity.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

import static javax.management.timer.Timer.ONE_MINUTE;

/**
 * @author Saurabh Gangamwar
 */
public class DeduplicationWithStateCleanUp {
    public static void main(String[] args) throws Exception {
        List<Order> ordersList = Arrays.asList(
                new Order(1, "John","apple"),
                new Order(2, "Chris","tomato"),
                new Order(3, "Ethan","mango"),
                new Order(1, "John","apple"), // Duplicate
                new Order(4, "Chris","banana"),
                new Order(3, "Ethan","mango") // Duplicate
        );

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Order> ordersDataStream = env.fromData(ordersList);

        DataStream<Order>  unqiueOrdersDataStream = ordersDataStream
                .keyBy(order -> order.orderId)
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<Order> previousOrderState;
                    private transient ValueState<Long> timerState;

                    @Override
                    public void open(OpenContext openContext) {
                        ValueStateDescriptor<Order> previousIbedRecStateDescriptor = new ValueStateDescriptor<>("previousOrderState", TypeInformation.of(Order.class));
                        previousOrderState = getRuntimeContext().getState(previousIbedRecStateDescriptor);
                        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                                "timer-state",
                                Types.LONG);
                        timerState = getRuntimeContext().getState(timerDescriptor);
                    }

                    @Override
                    public void processElement(Order order, KeyedProcessFunction<Integer, Order, Order>.Context context, Collector<Order> collector) throws Exception {
                        Order previousOrder = previousOrderState.value();
                        if (previousOrder == null) {
                            collector.collect(order);
                            previousOrderState.update(order);
                        } else {
                            if (previousOrder.orderId != order.orderId) {
                                collector.collect(order);
                                previousOrderState.update(order);
                            }
                        }

                        // set the timer and timer state
                        long timer = context.timerService().currentProcessingTime() + ONE_MINUTE * 10;
                        context.timerService().registerProcessingTimeTimer(timer);
                        timerState.update(timer);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, Order, Order>.OnTimerContext ctx, Collector<Order> out) {
                        // remove previous state after 10 minute
                        System.out.println("Timer fired!");
                        timerState.clear();
                        previousOrderState.clear();
                    }
                });

        unqiueOrdersDataStream.print();

        env.execute("Deduplication Job");
    }
}
