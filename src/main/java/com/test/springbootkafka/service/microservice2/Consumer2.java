package com.test.springbootkafka.service.microservice2;


import com.test.springbootkafka.model.User;
import com.test.springbootkafka.service.microservice1.Consumer;
import com.test.springbootkafka.service.microservice1.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@EnableKafka
public class Consumer2 {

    public Producer2 producer;
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    public Consumer2(Producer2 producer) {
        this.producer = producer;
    }

    @KafkaListener(topics ="kafkaTopic", groupId = "groupRania")
    public void consume(User user){
        // LOGGER.info(String.format("Message from test topic received", user.toString()));
        LOGGER.info(String.format(" Consumer read data from kafkaTopic", user.toString()));
        producer.sendMessage(user);
    }

}
