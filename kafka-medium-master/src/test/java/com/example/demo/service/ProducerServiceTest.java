package com.example.demo.service;

import static org.mockito.MockitoAnnotations.openMocks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.example.demo.model.TransactionMessage;
import com.fasterxml.jackson.core.JsonProcessingException;

@EmbeddedKafka
@SpringBootTest(properties = "kafka.servers=${spring.embedded.kafka.brokers}")
public class ProducerServiceTest {

	KafkaTemplate<String, String> kafkaTemplate;

	KafkaTemplate<String, TransactionMessage> transactionKafkaTemplate;

	@InjectMocks
	private ProducerService service;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	private TransactionMessage transaction;

	@BeforeEach
	public void setup() {
		openMocks(this);
		producerStringFactoryString();
		producerJsonFactoryString();

		service = new ProducerService(kafkaTemplate, transactionKafkaTemplate);
	}

//String Producer

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void sentStringProducer_success() {
		List<String> keys = new ArrayList<>();
		keys.add("Key1");
		keys.add("Key2");
		keys.add("Key3");
		SendResult<String, String> sendResult = Mockito.mock(SendResult.class);
		ListenableFuture<SendResult<String, String>> responseFuture = Mockito.mock(ListenableFuture.class);

		Mockito.doAnswer(invocationOnMock -> {
			ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
			listenableFutureCallback.onSuccess(sendResult);
			Assertions.assertEquals(sendResult.getRecordMetadata().offset(), 0);
			Assertions.assertEquals(sendResult.getRecordMetadata().partition(), 0);
			return null;
		}).when(responseFuture).addCallback(Mockito.any(ListenableFutureCallback.class));

		service.sendMessage("topic", keys, "record");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test()
	public void sendStringProducer_failure() {
		List<String> keys = new ArrayList<>();
		keys.add("Key1");
		keys.add("Key2");
		keys.add("Key3");

		ListenableFuture<SendResult<String, String>> responseFuture = Mockito.mock(ListenableFuture.class);
		Throwable throwable = Mockito.mock(Throwable.class);

		Mockito.when(throwable.getMessage()).thenReturn("message");
		Mockito.doAnswer(invocationOnMock -> {
			ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
			listenableFutureCallback.onFailure(throwable);
			Assertions.assertEquals(throwable.getMessage(), "message");
			return null;
		}).when(responseFuture).addCallback(Mockito.any(ListenableFutureCallback.class));

		service.sendMessage("topic", keys, "record");
	}

//Json Producer

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void sentTransactionProducer_success() throws JsonProcessingException {
		transaction = buildTransactionData();

		List<String> keys = new ArrayList<>();
		keys.add("Key1");
		keys.add("Key2");
		keys.add("Key3");

		SendResult<String, String> sendResult = Mockito.mock(SendResult.class);
		ListenableFuture<SendResult<String, String>> responseFuture = Mockito.mock(ListenableFuture.class);

		Mockito.doAnswer(invocationOnMock -> {
			ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
			listenableFutureCallback.onSuccess(sendResult);
			Assertions.assertEquals(sendResult.getRecordMetadata().offset(), 0);
			Assertions.assertEquals(sendResult.getRecordMetadata().partition(), 0);
			return null;
		}).when(responseFuture).addCallback(Mockito.any(ListenableFutureCallback.class));

		service.sendCustomMessage("topic", keys, transaction);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test()
	public void sentTransactionProducer_failure() throws JsonProcessingException {
		transaction = buildTransactionData();

		List<String> keys = new ArrayList<>();
		keys.add("Key1");
		keys.add("Key2");
		keys.add("Key3");

		ListenableFuture<SendResult<String, String>> responseFuture = Mockito.mock(ListenableFuture.class);
		Throwable throwable = Mockito.mock(Throwable.class);

		Mockito.when(throwable.getMessage()).thenReturn("message");
		Mockito.doAnswer(invocationOnMock -> {
			ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
			listenableFutureCallback.onFailure(throwable);
			Assertions.assertEquals(throwable.getMessage(), "message");
			return null;
		}).when(responseFuture).addCallback(Mockito.any(ListenableFutureCallback.class));

		service.sendCustomMessage("topic", keys, transaction);
	}

	private TransactionMessage buildTransactionData() {
		return TransactionMessage.builder().name("TestMessage").type("type").build();

	}

	private KafkaTemplate<String, String> producerStringFactoryString() {
		Map<String, Object> configProps = new HashMap<>();

		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		ProducerFactory<String, String> producerFactoryString = new DefaultKafkaProducerFactory<>(configProps);
		kafkaTemplate = new KafkaTemplate<>(producerFactoryString);
		return kafkaTemplate;
	}

	private KafkaTemplate<String, TransactionMessage> producerJsonFactoryString() {
		Map<String, Object> configProps = new HashMap<>();

		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		ProducerFactory<String, TransactionMessage> producerFactoryJson = new DefaultKafkaProducerFactory<>(
				configProps);
		transactionKafkaTemplate = new KafkaTemplate<>(producerFactoryJson);
		return transactionKafkaTemplate;
	}

}
