package com.sm.streaming;

import com.sm.streaming.serde.JsonDeserializer;
import com.sm.streaming.serde.JsonSerializer;
import com.sm.streaming.serde.WrapperSerde;
import com.sm.streaming.stockstats.model.Trade;
import com.sm.streaming.stockstats.model.TradeStats;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class StockStatsApp {

    Logger logger = LoggerFactory.getLogger(StockStatsApp.class);

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-stats-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //127.0.0.1
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        // Time interval, in millisecond, for our aggregation window
        long windowSize = 2000;

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,Trade> source = builder.stream(Constants.STOCK_TOPIC);
        KStream<String,Trade> source2 = builder.stream(Constants.STOCK_TOPIC);



        KStream<Windowed<String>,TradeStats> stats = source.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(windowSize))) //.advanceBy(Duration.ofSeconds(1))
                .aggregate(() -> new TradeStats(),(k,v,tradeStats)-> tradeStats.add(v),
                        Materialized.<String,TradeStats, WindowStore<Bytes,byte[]>>as("trade-aggregates3")
                                .withValueSerde(new TradeStatsSerde())).toStream().mapValues((trade)-> trade.computeAvgPrice());

        stats.to("stockstats2", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize)));

        KafkaStreams stream = new KafkaStreams(builder.build(),config);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

    static public final class TradeSerde extends WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<TradeStats>(), new JsonDeserializer<TradeStats>(TradeStats.class));
        }
    }
}
