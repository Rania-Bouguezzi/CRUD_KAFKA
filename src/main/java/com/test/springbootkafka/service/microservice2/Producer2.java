package com.test.springbootkafka.service.microservice2;

import com.test.springbootkafka.model.User;
import com.test.springbootkafka.service.microservice1.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;


@Service
public class Producer2 {


    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private final KafkaTemplate<String, User> kafkaTemplate;

    public Producer2(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User data) {
        LOGGER.info(String.format("Message sent to endPoint Topic  -> %s", data.toString()));


        Message<User> message = MessageBuilder.withPayload(data).setHeader(KafkaHeaders.TOPIC, "endPoint").build();

        kafkaTemplate.send(message);
    }
}