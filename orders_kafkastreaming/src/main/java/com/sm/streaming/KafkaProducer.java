package com.sm.streaming;

import com.sm.streaming.serde.JsonSerializer;
import com.sm.streaming.stockstats.model.Trade;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Component
public class KafkaProducer {

    private final KafkaTemplate<String, Trade> kafkaTemplate;
    private final String TOPIC_NAME= "stocks"; // Replace with your desired topic name

    public KafkaProducer(KafkaTemplate<String, Trade> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) throws Exception {
        //kafkaTemplate.send(TOPIC_NAME, message);


        Random random = new Random();
        long iter = 0;

        Map<String, Integer> prices = new HashMap<>();

        for (String ticker : Constants.TICKERS)
            prices.put(ticker, Constants.START_PRICE);

        // Start generating events, stop when CTRL-C
    int i =10;
        while (i>0) {
            iter++;
            for (String ticker : Constants.TICKERS) {
                double log = random.nextGaussian() * 0.25 + 1; // random var from lognormal dist with stddev = 0.25 and mean=1
                int size = random.nextInt(100);
                int price = prices.get(ticker);

                // flunctuate price sometimes
                if (iter % 10 == 0) {
                    price = price + random.nextInt(Constants.MAX_PRICE_CHANGE * 2) - Constants.MAX_PRICE_CHANGE;
                    prices.put(ticker, price);
                }

                Trade trade = new Trade("ASK",ticker,(price+log),size);
                // Note that we are using ticker as the key - so all asks for same stock will be in same partition
                ProducerRecord<String, Trade> record = new ProducerRecord<>(Constants.STOCK_TOPIC, ticker, trade);
                JsonSerializer<Trade> tradeSerializer = new JsonSerializer<>();

                System.out.println("record:"+record.key()+" val:"+record.value().toString());
                kafkaTemplate.send(record);

                // Sleep a bit, otherwise it is frying my machine
                Thread.sleep(Constants.DELAY);
            }
            System.out.print("val of i="+i);
            i=i-1;
        }
    }
}