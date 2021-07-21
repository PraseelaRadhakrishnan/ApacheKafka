package com.example.demo.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.model.TransactionMessage;
import com.example.demo.service.ProducerService;
import com.fasterxml.jackson.core.JsonProcessingException;

@RestController
@RequestMapping("/kafka")
public final class KafkaController {
    private final ProducerService producerService;
    
    @Value("${kafka.topic}")
	private String topic1;
    
    @Value("${kafka.topic1}")
	private String topic2;
    
    public KafkaController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam List<String> keys, @RequestParam String message) {
        producerService.sendMessage(topic1, keys,message);
    }
    
    @PostMapping(value = "/publishTransaction")
	public void sendJsonToKafkaTopic(@RequestParam List<String> keys, @RequestBody TransactionMessage message)
			throws JsonProcessingException {
        producerService.sendCustomMessage(topic2, keys, message);
    }
}
