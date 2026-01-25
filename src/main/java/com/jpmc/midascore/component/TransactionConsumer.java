package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
	
@Component
public class TransactionConsumer {
	@Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    @Transactional
    public void listen(Transaction transaction) {
    	// 1. Validate Sender
        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        
        // 2. Validate Recipient
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        // 3. Validate Balance
        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            
            // Deduct from sender
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            userRepository.save(sender);

            // Add to recipient
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());
            userRepository.save(recipient);

            // Record the transaction
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRepository.save(record);
            
            System.out.println("Transaction Processed: " + transaction.getAmount());
        } else {
            System.out.println("Transaction Rejected: " + transaction.toString());
        }
    }
}