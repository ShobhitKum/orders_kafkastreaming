package com.sm.streaming;

import com.sm.streaming.joiner.OrderPaymentJoiner;
import com.sm.streaming.joiner.OrderPymtItemJoiner;
import com.sm.streaming.model.*;
import com.sm.streaming.serde.JsonDeserializer;
import com.sm.streaming.serde.JsonSerializer;
import com.sm.streaming.serde.WrapperSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class OrderProcessingApp {


    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-join-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //127.0.0.1
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OrderModel> orders = builder.stream(Constants.ORDER_TOPIC, Consumed.with(Serdes.String(),new OrderSerde()));
        KTable<String, ItemsPriceModel> itemPrice = builder.table(Constants.ITEM_PRICE_TOPIC, Consumed.with(Serdes.String(), new ItemPriceSerde()));
        KStream<String, PaymentModel> payments = builder.stream(Constants.PAYMENT_TOPIC, Consumed.with(Serdes.String(), new PaymentSerde()));
        //repartitioning
        KStream<String, PaymentModel> repartitionedByOrderId = payments.selectKey((k, payment)-> {return payment.orderId;});
        repartitionedByOrderId.peek((k,v)-> System.out.println("Repartitioned with k="+k+" v orderid="+v.orderId+" paymentid="+v.paymentId));
        //repartitionedByOrderId.to("stockstats2");
        JoinWindows tenMinWindow =  JoinWindows.of(Duration.ofMinutes(10));



        //value joiner for order and payment join
        ValueJoiner<OrderModel, PaymentModel, OrderPaymentModel> orderPaymentJoiner = new OrderPaymentJoiner();


        KStream<String, OrderPaymentModel> order_payment_stream = orders.join(repartitionedByOrderId,orderPaymentJoiner,tenMinWindow, StreamJoined.with(Serdes.String(),new OrderSerde(),new PaymentSerde()));

        order_payment_stream.print(Printed.<String, OrderPaymentModel>toSysOut().withLabel(" joined order and payment"));

        //repartitioning to join with items before ***
        KStream<String, OrderPaymentModel> orderpayment_byitem_stream = order_payment_stream.selectKey((k, v)-> {return v.itemId;});



        //value joiner for final order payment and item
        ValueJoiner<OrderPaymentModel, ItemsPriceModel, OrderPymtItemModel> orderpymtItemJoiner = new OrderPymtItemJoiner();



        KStream<String, OrderPymtItemModel> orderpayment_item_stream = orderpayment_byitem_stream.leftJoin(itemPrice,orderpymtItemJoiner,Joined.with(Serdes.String(),new OrderPaymentSerde(),new ItemPriceSerde()));

        orderpayment_item_stream.peek((k,v)-> System.out.println("orderpayment_item_stream with k="+k+" v as ="+v.toString()));

        orderpayment_item_stream.print(Printed.<String, OrderPymtItemModel>toSysOut().withLabel(" joined order_payment with items"));
        orderpayment_item_stream.selectKey((k,v) -> {return  v.orderId;}).to("stockstats2", Produced.with(Serdes.String(),new OrderPymtItemPojoSerde()));




        KafkaStreams stream = new KafkaStreams(builder.build(),config);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }




    static public final class ItemPriceSerde extends WrapperSerde<ItemsPriceModel> {
        public ItemPriceSerde() {
            super(new JsonSerializer<ItemsPriceModel>(), new JsonDeserializer<ItemsPriceModel>(ItemsPriceModel.class));
        }
    }

    static public final class OrderSerde extends WrapperSerde<OrderModel> {
        public OrderSerde() {
            super(new JsonSerializer<OrderModel>(), new JsonDeserializer<OrderModel>(OrderModel.class));
        }
    }

    static public  final class PaymentSerde extends  WrapperSerde<PaymentModel>{
        public PaymentSerde() {
            super(new JsonSerializer<PaymentModel>(), new JsonDeserializer<PaymentModel>(PaymentModel.class));
        }
    }

    static public final class OrderPaymentSerde extends WrapperSerde<OrderPaymentModel> {
        public OrderPaymentSerde() {
            super(new JsonSerializer<OrderPaymentModel>(), new JsonDeserializer<OrderPaymentModel>(OrderPaymentModel.class));
        }
    }

    static public final class OrderPymtItemPojoSerde extends WrapperSerde<OrderPymtItemModel> {
        public OrderPymtItemPojoSerde() {
            super(new JsonSerializer<OrderPymtItemModel>(), new JsonDeserializer<OrderPymtItemModel>(OrderPymtItemModel.class));
        }
    }

}


