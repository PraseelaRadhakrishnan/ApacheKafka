package com.example.demo.controller;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.example.demo.model.TransactionMessage;
import com.example.demo.service.ProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;

@WebMvcTest(KafkaController.class)
public class KafkaControllerTest {

	@Autowired
	MockMvc mockMvc;

	@MockBean
	private ProducerService producerService;

	private String transaction;
	
	@Autowired
    private ObjectMapper objectMapper;

	@BeforeEach
	public void setup() {
		openMocks(this);

	}

	@Test
	public void happyPath_insertString() throws Throwable {

		MvcResult mvcResult = this.mockMvc.perform(post("/kafka/publish")
				.param("keys", "key1")
				.param("keys", "key2")
				.param("message", "transaction"))
				.andExpect(status().isOk()).andReturn();

		String actualResponse = mvcResult.getResponse().getContentAsString();
		int statusCode = mvcResult.getResponse().getStatus();

		assertNotNull(actualResponse);
		Assertions.assertEquals(statusCode, 200);
	}

	@Test
	public void happyPath_insertJson() throws Throwable {

		transaction = objectMapper.writeValueAsString(buildTransactionData());
		MvcResult mvcResult = this.mockMvc.perform(post("/kafka/publishTransaction")
				.param("keys", "key")
				.contentType(MediaType.APPLICATION_JSON)
                .content(transaction))
				.andExpect(status().isOk()).andReturn();

		String actualResponse = mvcResult.getResponse().getContentAsString();
		int statusCode = mvcResult.getResponse().getStatus();

		assertNotNull(actualResponse);
		Assertions.assertEquals(statusCode, 200);
	}
	
	private TransactionMessage buildTransactionData() {
		return TransactionMessage.builder().name("TestMessage").type("type").build();

	}
}
