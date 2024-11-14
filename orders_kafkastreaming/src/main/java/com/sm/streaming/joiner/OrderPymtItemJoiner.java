package com.sm.streaming.joiner;

import com.sm.streaming.model.ItemsPriceModel;
import com.sm.streaming.model.OrderPaymentModel;
import com.sm.streaming.model.OrderPymtItemModel;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class OrderPymtItemJoiner implements ValueJoiner<OrderPaymentModel, ItemsPriceModel, OrderPymtItemModel> {
    @Override
    public OrderPymtItemModel apply(OrderPaymentModel orderPaymentPojo, ItemsPriceModel itemPrice) {
        String orderId = null;
        int orderQty = 0;
        String paymentId = null;
        String itemId = null;
        double txnAmount = 0;
        String status = null;

        if (orderPaymentPojo != null) {
            orderId = orderPaymentPojo.orderId;
            orderQty = orderPaymentPojo.orderQty;
            paymentId = orderPaymentPojo.paymentId;
            itemId = orderPaymentPojo.itemId;
            txnAmount = orderPaymentPojo.txnAmount;
            status = orderPaymentPojo.status;
        }

        String itemName = itemPrice != null ? itemPrice.itemName : null;
        double itemPrc = itemPrice != null ? itemPrice.price : 0.0;

        OrderPymtItemModel opi = new OrderPymtItemModel(orderId,orderQty,paymentId,itemId,txnAmount,status,itemName,itemPrc);


        return opi;
    }
}
