package com.example.demo.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.example.demo.model.TransactionMessage;
import com.fasterxml.jackson.core.JsonProcessingException;

@Service
public final class ProducerService {
	private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final KafkaTemplate<String, TransactionMessage> transactionKafkaTemplate;

	public ProducerService(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, TransactionMessage> transactionKafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
		this.transactionKafkaTemplate = transactionKafkaTemplate;
	}

	public void sendMessage(String topic, List<String> keys, String message) {

		for (String key : keys) {
			logger.info(String.format("$$$$ => Producing message :%s %s", key, message));

			ListenableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topic, key, message);
			future.addCallback(new ListenableFutureCallback<>() {
				@Override
				public void onFailure(Throwable ex) {
					logger.info("Unable to send message=[ {} ] due to : {}", message, ex.getMessage());
				}

				@Override
				public void onSuccess(SendResult<String, String> result) {
					logger.info("Sent key=[{}] message=[ {} ] with offset=[ {} ]", key, message,
							result.getRecordMetadata().offset());
				}
			});
		}

	}

	public void sendCustomMessage(String topic, List<String> keys, TransactionMessage transaction) throws JsonProcessingException {
		for (String key : keys) {
			logger.info(String.format("$$$$ => Producing Json message :%s %s", key, transaction));

			ListenableFuture<SendResult<String, TransactionMessage>> futureListenableFuture = this.transactionKafkaTemplate.send(topic, key,
					transaction);
			futureListenableFuture.addCallback(new ListenableFutureCallback<>() {
				@Override
				public void onFailure(Throwable ex) {
					logger.info("Unable to send message=[ {} ] due to : {}", transaction.getName(), ex.getMessage());
				}

				@Override
				public void onSuccess(SendResult<String, TransactionMessage> result) {
					logger.info("Sent key=[{}] message=[ {} ] with offset=[ {} ]", key, transaction.getName(),
							result.getRecordMetadata().offset());
				}
			});
		}
	}
}
