package com.sm.streaming.model;

public class OrderPymtItemModel {

    public String orderId;
    public int orderQty;
    public String paymentId;
    public String itemId;
    public double txnAmount;
    public String status;
    public String itemName;
    public double itemPrice;


    public OrderPymtItemModel(String orderId, int orderQty, String paymentId, String itemId, double txnAmount, String status, String itemName, double itemPrice) {
        this.orderId = orderId;
        this.orderQty = orderQty;
        this.paymentId = paymentId;
        this.itemId = itemId;
        this.txnAmount = txnAmount;
        this.status = status;
        this.itemName = itemName;
        this.itemPrice = itemPrice;
    }

    @Override
    public String toString() {
        return "OrderPymtItemPojo{" +
                "orderId='" + orderId + '\'' +
                ", orderQty=" + orderQty +
                ", paymentId='" + paymentId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", txnAmount=" + txnAmount +
                ", status='" + status + '\'' +
                ", itemName='" + itemName + '\'' +
                ", itemPrice=" + itemPrice +
                '}';
    }
}
