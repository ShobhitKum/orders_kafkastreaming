package com.sm.streaming;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    @KafkaListener(topics = "input", groupId = "sm.input.group")
    public void consumeMessage(String message) {
        System.out.println("Received message: " + message);
    }
}
