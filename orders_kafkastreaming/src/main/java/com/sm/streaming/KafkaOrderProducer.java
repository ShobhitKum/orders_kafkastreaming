package com.sm.streaming;

import com.sm.streaming.model.ItemsPriceModel;
import com.sm.streaming.model.OrderModel;
import com.sm.streaming.model.PaymentModel;
import com.sm.streaming.util.ConstantDataGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaOrderProducer {

        @Autowired
        private KafkaTemplate<String, OrderModel> order_kafkatemplate;
        @Autowired
        private KafkaTemplate<String, ItemsPriceModel> item_kafkaTemplate;
        @Autowired
        private  KafkaTemplate<String, PaymentModel> payment_kafkaTemplate;



    public void sendMessage() throws Exception{
        ConstantDataGenerator.initItem();
        ConstantDataGenerator.initOrderAndPayment();


        ConstantDataGenerator.itemMap.entrySet().stream().forEach(e -> {
            String key = e.getKey();
            ItemsPriceModel val = e.getValue();
            ProducerRecord<String, ItemsPriceModel> record = new ProducerRecord(Constants.ITEM_PRICE_TOPIC, val.itemId, val);
            item_kafkaTemplate.send(record);
        } );


        ConstantDataGenerator.paymentMap.entrySet().stream().forEach(e -> {
            String key = e.getKey();
            PaymentModel val = e.getValue();
            ProducerRecord<String, PaymentModel> record = new ProducerRecord(Constants.PAYMENT_TOPIC, key, val);
            payment_kafkaTemplate.send(record);
        } );

        ConstantDataGenerator.orderMap.entrySet().stream().forEach(e -> {
            String key = e.getKey();
            OrderModel val = e.getValue();
            ProducerRecord<String, OrderModel> record = new ProducerRecord(Constants.ORDER_TOPIC, key, val);
            order_kafkatemplate.send(record);
        } );
    }



}
