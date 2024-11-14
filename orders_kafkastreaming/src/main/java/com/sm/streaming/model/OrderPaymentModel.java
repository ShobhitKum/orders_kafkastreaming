package com.sm.streaming.model;

public class OrderPaymentModel {

    public String orderId;
    public int orderQty;
    public String paymentId;
    public String itemId;
    public double txnAmount;
    public String status;

    public OrderPaymentModel(String orderId, int orderQty, String paymentId, String itemId, double txnAmount, String status) {
        this.orderId = orderId;
        this.orderQty = orderQty;
        this.paymentId = paymentId;
        this.itemId = itemId;
        this.txnAmount = txnAmount;
        this.status = status;
    }

    @Override
    public String toString() {
        return "OderWithPayment{" +
                "orderId='" + orderId + '\'' +
                ", orderQty=" + orderQty +
                ", paymentId='" + paymentId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", txnAmount=" + txnAmount +
                ", status='" + status + '\'' +
                '}';
    }
}
