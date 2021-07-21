package com.example.demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.example.demo.model.TransactionMessage;

@Service
public final class ConsumerService {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

	@KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.consumerGroupId1}", 
			containerFactory = "stringWithoutFilterKafkaListenerContainerFactory")

	public void stringWithoutFilterListener(String record, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {

		logger.info(String.format("$$$$ => Consumed message without Filter: %s %s", record, key));

	}

	@KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.consumerGroupId2}", 
			containerFactory = "stringFilterKafkaListenerContainerFactory")

	public void stringFilterListener(String record, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {

		logger.info(String.format("$$$$ => Consumed message with filter: %s %s", record, key));

	}

	@KafkaListener(topics = "${kafka.topic1}", groupId = "${kafka.consumerGroupId1}", containerFactory = "jsonFilterKafkaListenerContainerFactory")

	void jsonListener(TransactionMessage transaction, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {

		logger.info("CustomUserListener with filter [{}] [{}]", transaction, key);
	}
}
