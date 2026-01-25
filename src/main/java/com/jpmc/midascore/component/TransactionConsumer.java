package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void listen(Transaction transaction) {
        // This print statement is what you are looking for!
        System.out.println("Received transaction: " + transaction.toString());
    }
}