package com.test.springbootkafka.service;

import com.test.springbootkafka.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class testProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(testProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;

    public testProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topicName, String message){

        LOGGER.info(String.format("Message sent -> %s" , topicName, message));

       // kafkaTemplate.send("groupRania" , message);


         kafkaTemplate.send(topicName,message);
    }
}



