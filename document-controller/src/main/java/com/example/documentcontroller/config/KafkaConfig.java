package com.example.documentcontroller.config;

import com.example.documentcontroller.kafka.FilteringReplyingKafkaTemplate;
import com.example.documentcontroller.model.Document;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

@Configuration
@Slf4j
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.client-id}")
    private String producerClientId;

    @Value("${kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.consumer.client-id}")
    private String consumerClientId;

    @Value("${documents.topic}")
    private String documentsTopic;

    @Value("${documents.reply-timeout}")
    private Long documentsReplyTimeout;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId);
        return props;
    }

    @Bean
    public ProducerFactory<String, Document> producerFactory() {
        JsonSerializer<Document> valueSerializer = new JsonSerializer<Document>().noTypeInfo();
        return new DefaultKafkaProducerFactory<>(producerConfigs(), new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, Document> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, Document> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), new JsonDeserializer<>(Document.class));
    }

    @Bean
    public KafkaMessageListenerContainer<String, Document> replyContainer(ConsumerFactory<String, Document> cf) {
        ContainerProperties containerProperties = new ContainerProperties(documentsTopic);
        return new KafkaMessageListenerContainer<>(cf, containerProperties);
    }

    @Bean
    public FilteringReplyingKafkaTemplate<String, Document, Document> replyingKafkaTemplate(ProducerFactory<String, Document> pf, KafkaMessageListenerContainer<String, Document> container) {
        final Predicate<ConsumerRecord<String, Document>> consumerRecordFilter = consumerRecord -> {
            return "DONE".equals(consumerRecord.value().getStatus());
        };
        var template = new FilteringReplyingKafkaTemplate<>(pf, container, consumerRecordFilter);
        template.setReplyTimeout(documentsReplyTimeout);
        return template;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Document>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Document> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setReplyTemplate(kafkaTemplate());
        return factory;
    }

}