package com.example.demo.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.demo.model.TransactionMessage;

@EnableKafka
@Configuration
public class KafkaConfiguration {
	
	@Value("${kafka.server}")
	private String server;

	@Value("${kafka.api.key}")
	private String key;

	@Value("${kafka.consumerGroupId1}")
	private String consumerGroupId1;
	
	@Value("${kafka.consumerGroupId2}")
	private String consumerGroupId2;

//Producer

	@Bean
	public ProducerFactory<String, String> producerFactoryString() {
		Map<String, Object> configProps = new HashMap<>();

		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplateString() {
		return new KafkaTemplate<>(producerFactoryString());
	}

//Consumer

	@Bean
	public ConsumerFactory<String, String> consumerFactoryWithFilter() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId1);
		configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return new DefaultKafkaConsumerFactory<>(configProps);
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> stringFilterKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryWithFilter());
		factory.setAckDiscarded(true);
		factory.setRecordFilterStrategy(consumeMessage -> 
			!(consumeMessage.key().contains(key)));
		return factory;
	}
	
	@Bean
	public ConsumerFactory<String, String> consumerFactoryWithoutFilter() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId1);
		configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return new DefaultKafkaConsumerFactory<>(configProps);
	}
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> stringWithoutFilterKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryWithoutFilter());
		return factory;
	}

	@Bean
	public ProducerFactory<String, TransactionMessage> transactionProducerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, TransactionMessage> transactionKafkaTemplate() {
		return new KafkaTemplate<>(transactionProducerFactory());
	}
	
	public ConsumerFactory<String, TransactionMessage> transactionConsumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId1);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.demo.model");
		
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TransactionMessage>> jsonFilterKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, TransactionMessage> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(transactionConsumerFactory());
		factory.setAckDiscarded(true);
		factory.setRecordFilterStrategy(record -> 
			!(record.key().contains(key)));
		return factory;
	}
}