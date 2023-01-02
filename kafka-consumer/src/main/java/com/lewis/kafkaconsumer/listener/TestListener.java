package com.lewis.kafkaconsumer.listener;


import com.lewis.kafkaconsumer.models.Person;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;

@Configuration
public class TestListener {


    @KafkaListener(topics = "person-example-topic", groupId = "group-1", containerFactory =
    "jsonKafkaListenerContainerFactory")
    public void personExample(Person person, ConsumerRecordMetadata metadata)
    {
        System.out.println("Topic : " + metadata.topic() + " partition: "
        + metadata.partition());
        System.out.println(" Person name : " + person.getName() + " Person Age: " + person.getAge());
        System.out.println("-------------------------------------------");

    }

}
