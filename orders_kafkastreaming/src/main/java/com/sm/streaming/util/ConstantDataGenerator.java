package com.sm.streaming.util;

import com.sm.streaming.Constants;
import com.sm.streaming.model.ItemsPriceModel;
import com.sm.streaming.model.OrderModel;
import com.sm.streaming.model.PaymentModel;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class ConstantDataGenerator {


    public static Map<String, ItemsPriceModel> itemMap = new HashMap<String, ItemsPriceModel>();
    public static Map<String, PaymentModel> paymentMap = new HashMap<String, PaymentModel>();
    public static Map<String, OrderModel> orderMap = new HashMap<String, OrderModel>();



    public static void initItem(){
        int itemId=1001;
        Random itemPrice =new Random();
        for(String s: Constants.ITEM_CATEGORY) {
            ItemsPriceModel item = new ItemsPriceModel(String.valueOf(itemId),s,itemPrice.nextInt(100));
            itemMap.put(item.itemName,item);
            itemId++;
        }
    }

    public static void initOrderAndPayment(){
        String orderId = "ORD_";
        String paymentID = "PYMT_";
        int int_paymtId=20000;
        int int_ordId = 10000;
        Random idGen =new Random();
        Random txnAmount =new Random();

        int i =100;
        while(i>0){
            for(String s: Constants.ITEM_CATEGORY){
                int val = idGen.nextInt(100);
                String paymentId = paymentID+int_paymtId+val;
                String ordId = orderId+int_ordId+val;
                double amount = txnAmount.nextInt(100);
                OrderModel order = new OrderModel(ordId,10, new Date(),paymentId,itemMap.get(s).itemId);
                PaymentModel payment = new PaymentModel(paymentId,amount, new Date(),Constants.SUCCESS, ordId);
                orderMap.put(ordId,order);
                paymentMap.put(paymentId,payment);
                System.out.println(ordId);
            }

            i--;
        }


    }

    public static void main(String[] args) {
        //ConstantDataGenerator cdg = new ConstantDataGenerator();
        ConstantDataGenerator.initItem();
        ConstantDataGenerator.initOrderAndPayment();
        System.out.println(paymentMap);
        System.out.println(orderMap);
        System.out.println(itemMap);
    }



}
