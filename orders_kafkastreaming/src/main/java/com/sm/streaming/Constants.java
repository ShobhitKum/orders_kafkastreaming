package com.sm.streaming;

public class Constants {
    public static final String STOCK_TOPIC = "stocks2";
    public static final String[] TICKERS = {"APPLE", "GS", "ABBV", "ACN", "ATVI", "AYI", "ADBE", "AAP", "AES", "AET"};
    public static final int MAX_PRICE_CHANGE = 5;
    public static final String[] ITEM_CATEGORY = {"CARS", "BOOKS", "LAPTOP&ACCE", "BOX", "KITCHENELEC", "LIVINGROOM", "DECORATIONS", "FURNITURES", "GROCERRY", "WARDROBE"};
    public static final int START_PRICE = 5000;
    public static final int DELAY = 100; // sleep in ms between sending "asks"
    public static final String BROKER = "localhost:9092";
    public static final String SUCCESS = "SUCCESS";
    public static final String PENDING = "PENDING";
    public static final String FAILED = "FAILED";
    public static final String ORDER_TOPIC= "order"; // Replace with your desired topic name
    public static final String PAYMENT_TOPIC= "payment"; // Replace with your desired topic name
    public static final String ITEM_PRICE_TOPIC= "item-price"; // Replace with your desired topic name




}
