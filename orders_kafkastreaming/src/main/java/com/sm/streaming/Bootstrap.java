package com.sm.streaming;//package com.sm.com.sm.streaming;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.Named;
//import org.springframework.stereotype.Component;
//import java.util.Arrays;
//import java.util.Properties;
//
//@Component
//public class Bootstrap {
//
//    Properties config = new Properties();
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //127.0.0.1
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//
//    StreamsBuilder builder = new StreamsBuilder();
//    KStream<String, String> wordcountInput = builder.stream("input");
//    KTable<String,Long> wordCounts = wordcountInput.peek((k, v) -> System.out.println("Observed event:"+ v))
//            .mapValues(txtline -> txtline.toLowerCase())
//            .flatMapValues(lowerLines -> Arrays.asList(lowerLines.split(" "))).peek((k, v) -> System.out.println(k+" Final key with val="+ v))
//            .selectKey((k,v)->v).groupByKey().count(Named.as("counts"));
//
//        wordCounts.toStream().to("output");
//
//    KafkaStreams stream = new KafkaStreams(builder.build(),config);
//        stream.start();
//
//    //printed the topology
//        System.out.println(stream.toString());
//
//    //closing stream application
//        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
//
//}
