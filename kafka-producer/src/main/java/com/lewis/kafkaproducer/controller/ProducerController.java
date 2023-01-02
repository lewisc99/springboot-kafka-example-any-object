package com.lewis.kafkaproducer.controller;


import com.lewis.kafkaproducer.models.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.time.LocalDateTime;

@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, Serializable> jsonKafkaTemplate;


    @GetMapping("send-person")
    public void sendPerson()
    {
        System.out.println(LocalDateTime.now());
        jsonKafkaTemplate.send("person-example-topic", new Person("Maria",80));
    }


}
