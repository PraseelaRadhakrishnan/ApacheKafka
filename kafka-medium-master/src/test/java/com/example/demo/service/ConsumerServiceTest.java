package com.example.demo.service;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.MockitoAnnotations.openMocks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;

import com.example.demo.model.TransactionMessage;

public class ConsumerServiceTest {

	@InjectMocks
	private ConsumerService consumerService;

	private TransactionMessage transaction;

	@BeforeEach
	public void setup() {
		openMocks(this);
	}

	@Test
	public void consumerStringTest() throws Throwable {
		String record = "data";

		try {
			consumerService.stringFilterListener(record, "key");
		} catch (Exception e) {

			fail("Success");
		}
	}

	@Test
	public void consumerJsonTest() throws Throwable {
		transaction = buildTransactionData();

		try {
			consumerService.jsonListener(transaction, "key");
		} catch (Exception e) {

			fail("Success");
		}
	}

	private TransactionMessage buildTransactionData() {
		return TransactionMessage.builder().name("TestMessage").type("type").build();

	}

}
