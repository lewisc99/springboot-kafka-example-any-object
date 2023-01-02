package com.lewis.kafkaconsumer.listener;


import com.lewis.kafkaconsumer.models.Person;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class TestListener {


    @KafkaListener(topics = "person-example-topic", groupId = "group-1", containerFactory =
    "jsonKafkaListenerContainerFactory")
    public void personExample(Person person, ConsumerRecordMetadata metadata)
    {
        System.out.println("Topic : " + metadata.topic() + " partition: "
        + metadata.partition() + " groupId: group-1 ");
        System.out.println(" Person name : " + person.getName() + " Person Age: " + person.getAge());
        System.out.println("-------------------------------------------");

    }

     @KafkaListener(topics = "person-example-topic", groupId = "group-2", containerFactory = "jsonKafkaListenerContainerFactory")
     @RetryableTopic(attempts = "4", dltTopicSuffix = ".dlt", backoff = @Backoff(delay = 2000, multiplier = 3.0)  )
     @SendTo("person-example-topic.DLT")
    public void personExampleDifferentGroupId(Person person, ConsumerRecordMetadata metadata)
    {

        System.out.println("Topic : " + metadata.topic() + " partition: "
                + metadata.partition() + " groupId: group-2 ");
        System.out.println(" Person name : " + person.getName() + " Person Age: " + person.getAge());
        System.out.println("-------------------------------------------");
        throw new IllegalArgumentException("fail listener");
    }

    @DltHandler
    public void dltSecond(Person person, ConsumerRecordMetadata metadata)
    {
        System.out.println("DLT messages errors " + person.getName());
    }

}
