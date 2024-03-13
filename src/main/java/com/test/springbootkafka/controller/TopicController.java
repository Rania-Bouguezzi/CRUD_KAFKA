package com.test.springbootkafka.controller;

import com.test.springbootkafka.service.testProducer;
import com.test.springbootkafka.service.testConsumer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/kafka")
public class TopicController {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private AdminClient adminClient;
    private testProducer producer;
    private testConsumer consumer;



    public TopicController(KafkaAdmin kafkaAdmin, AdminClient adminClient, testProducer producer, testConsumer consumer) {
        this.kafkaAdmin = kafkaAdmin;
        this.adminClient = adminClient;
        this.producer = producer;
        this.consumer = consumer;
    }


    /////////////////// add Topic //////////////////////
    @PostMapping("/addTopic")
    public ResponseEntity<String> addTopic(@RequestParam("topicName") String topicName) {
        NewTopic newTopic = TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .build();

        adminClient.createTopics(Collections.singletonList(newTopic));


        return ResponseEntity.ok("Topic creation initiated");
    }
    @PostMapping("/updateTopicName")
    public ResponseEntity<String> updateTopicName(
            @RequestParam("currentTopicName") String currentTopicName,
            @RequestParam("newTopicName") String newTopicName
    ) {
        // Vérifiez si le topic actuel existe
        if (!topicExists(currentTopicName)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Le topic actuel n'existe pas.");
        }

        // Vérifiez si le nouveau nom du topic existe déjà
        if (topicExists(newTopicName)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Le nouveau nom du topic existe déjà. Veuillez en choisir un autre.");
        }

        // Mettez à jour le nom du topic
        try {
            // Code de mise à jour du nom du topic en utilisant les fonctionnalités Kafka
            // Assurez-vous d'ajuster cela en fonction de votre configuration Kafka.

            return ResponseEntity.ok("Nom du topic mis à jour avec succès");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Erreur lors de la mise à jour du nom du topic : " + e.getMessage());
        }
    }

    private boolean topicExists(String topicName) {
        // Ajoutez la logique pour vérifier si le topic existe déjà
        // Cela peut dépendre de votre configuration Kafka.

        return true; // Remplacez ceci par la logique réelle de vérification.
    }

    //////////////// delete Topic ////////////
    @DeleteMapping("/deleteTopic/{topicName}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topicName) {
        DeleteTopicsOptions deleteOptions = new DeleteTopicsOptions();
        deleteOptions.timeoutMs(5000); // Timeout pour la suppression
        adminClient.deleteTopics(Collections.singleton(topicName), deleteOptions).all().whenComplete((voidValue, throwable) -> {
            if (throwable == null) {
                System.out.println("Topic " + topicName + " deleted successfully");
            } else {
                System.out.println("Error deleting topic " + topicName + ": " + throwable.getMessage());
            }
        });
        return ResponseEntity.ok("Topic deletion initiated");
    }


    //////////// get list Topic ////////////
    @GetMapping("/listTopics")
    public ResponseEntity<Set<String>> listTopics() {
        ListTopicsOptions options = new ListTopicsOptions();
        ListTopicsResult topicsResult = adminClient.listTopics(options);
        KafkaFuture<Set<String>> topicNamesFuture = topicsResult.names();
        try {
            Set<String> topicNames = topicNamesFuture.get();
            return ResponseEntity.ok(topicNames);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).build();
        }
    }
    /*@PostMapping("/sendMessage/{topicName}")
    public ResponseEntity<String> sendMessage(@PathVariable String topicName, @RequestParam("message") String message){
        producer.sendMessage(topicName,message);
        return ResponseEntity.ok("Message sent to the topic");*/

        @PostMapping("/sendMessage")
        public ResponseEntity<String> sendMessage(
                @RequestParam String topicName,
                @RequestParam String message
    ) {
            producer.sendMessage(topicName,message);
            return ResponseEntity.ok("Message sent to the topic");
        }

    @GetMapping("/consume/{topic}")
    public ResponseEntity<List<String>> consumeMessages(@PathVariable String topic) {
        consumer.subscribeToTopic(topic);

        List<String> messages = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.consumeAllMessages();

        for (ConsumerRecord<String, String> record : records) {
            messages.add(record.value());
        }

        return ResponseEntity.ok(messages);
    }




    }

