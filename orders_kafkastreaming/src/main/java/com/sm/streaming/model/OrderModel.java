package com.sm.streaming.model;

import java.util.Date;

public class OrderModel {
    public String orderId;
    public int orderQty;
    public Date date;
    public String paymentId;
    public String itemId;

    public OrderModel(String orderId, int orderQty, Date date, String paymentId, String itemId) {
        this.orderId = orderId;
        this.orderQty = orderQty;
        this.date = date;

        this.paymentId = paymentId;
        this.itemId = itemId;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", orderQty=" + orderQty +
                ", date=" + date +
                ", paymentId='" + paymentId + '\'' +
                ", itemId='" + itemId + '\'' +
                '}';
    }
}
