package com.sm.streaming.model;

import java.util.Date;

public class PaymentModel {
    public String paymentId;
    public double txnAmount;
    public Date date;
    public String status;
    public String orderId;

    public PaymentModel(String paymentId, double txnAmount, Date date, String status, String orderId) {
        this.paymentId = paymentId;
        this.txnAmount = txnAmount;
        this.date = date;
        this.status = status;
        this.orderId = orderId;
    }

    @Override
    public String toString() {
        return "Payment{" +
                "paymentId='" + paymentId + '\'' +
                "orderId='" + orderId + '\'' +
                ", txnAmount=" + txnAmount +
                ", date=" + date +
                ", status='" + status + '\'' +
                '}';
    }
}
