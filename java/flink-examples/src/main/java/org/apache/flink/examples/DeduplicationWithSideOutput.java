package org.apache.flink.examples;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.examples.entity.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author Saurabh Gangamwar
 */
public class DeduplicationWithSideOutput {

    public static void main(String[] args) throws Exception {
        final OutputTag<Order> duplicateOrdersSideOutputTag = new OutputTag<Order>("duplicate-orders-side-output"){};

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

        SingleOutputStreamOperator<Order> uniqueOrdersDataStream = ordersDataStream
                .keyBy(order -> order.orderId)
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<Order> previousOrderState;

                    @Override
                    public void open(OpenContext openContext) {
                        ValueStateDescriptor<Order> previousIbedRecStateDescriptor = new ValueStateDescriptor<>("previousOrderState", TypeInformation.of(Order.class));
                        previousOrderState = getRuntimeContext().getState(previousIbedRecStateDescriptor);
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
                            } else {
                                // Send duplicate order to duplicatesOrders side-output
                                context.output(duplicateOrdersSideOutputTag, order);
                            }
                        }
                    }
                });

        DataStream<Order> duplicatesOrdersDataStream = uniqueOrdersDataStream.getSideOutput(duplicateOrdersSideOutputTag);

        duplicatesOrdersDataStream.print("Duplicate Order");

        uniqueOrdersDataStream.print("Unique Order");

        env.execute("Deduplication Job");
    }
}