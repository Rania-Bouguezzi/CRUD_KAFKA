package com.test.springbootkafka.service.microservice1;

import com.test.springbootkafka.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@EnableKafka
public class Consumer {

   public final Producer producer;


    public Consumer(Producer producer) {
        this.producer = producer;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

@KafkaListener(topics ="test", groupId = "groupRania")
public void consume(User user){
   // LOGGER.info(String.format("Message from test topic received", user.toString()));
    LOGGER.info(String.format(" Consumer read data", user.toString()));
    producer.sendMessage(user);


}





}
