package org.apache.flink.examples.entity;

/**
 * @author Saurabh Gangamwar
 */
public class Order {

    public int orderId;

    public String userName;

    public String product;

    public Order(int orderId, String userName, String product) {
        this.orderId = orderId;
        this.userName = userName;
        this.product = product;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", userName='" + userName + '\'' +
                ", product='" + product + '\'' +
                '}';
    }
}
