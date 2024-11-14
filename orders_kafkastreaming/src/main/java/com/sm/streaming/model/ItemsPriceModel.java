package com.sm.streaming.model;

public class ItemsPriceModel {
    public String itemId;
    public String itemName;
    public double price;

    public ItemsPriceModel(String itemId, String itemName, double price) {
        this.itemId = itemId;
        this.itemName = itemName;
        this.price = price;
    }

    @Override
    public String toString() {
        return "ItemPrice{" +
                "itemId='" + itemId + '\'' +
                ", itemName='" + itemName + '\'' +
                ", price=" + price +
                '}';
    }
}
