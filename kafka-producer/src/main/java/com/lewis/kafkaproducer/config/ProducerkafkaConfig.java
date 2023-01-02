package com.lewis.kafkaproducer.config;


import com.fasterxml.jackson.databind.JsonSerializable;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.io.Serializable;
import java.util.HashMap;

@Configuration
public class ProducerkafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;
    

    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        var configs = new HashMap<String,Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    @Bean
    public ProducerFactory<String,Object> jsonProducerFactory()
    {
        var configs = new HashMap<String, Object>();

        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializable.class);


        return new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new JsonSerializer<>());

    }

    @Bean
    public KafkaTemplate<String, Serializable> jsonKafkaTemplate()
    {
        return new KafkaTemplate(jsonProducerFactory());
    }

    @Bean
    KafkaAdmin.NewTopics topics()
    {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("person-example-topic").partitions(2).build()
        );
    }

}
