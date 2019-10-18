package com.example.documentcontroller.controller;

import com.example.documentcontroller.kafka.FilteringReplyingKafkaTemplate;
import com.example.documentcontroller.model.Document;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

@Controller
@Slf4j
public class DocumentController {
    @Value("${documents.topic}")
    private String documentsTopic;
    @Value("${documents.reply-timeout}")
    private long replyTimeout;

    private FilteringReplyingKafkaTemplate<String, Document, Document> kafkaTemplate;

    public DocumentController(FilteringReplyingKafkaTemplate<String, Document, Document> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(value = "/documents")
    public ResponseEntity<Mono<Document>> postDocument(@RequestBody Mono<Document> document) {
        Mono<Document> documentMono = document
            .map(this::setCreated)
            .flatMap(this::sendAndReceive)
            .map(ConsumerRecord::value);
        return new ResponseEntity<>(documentMono, HttpStatus.OK);
    }

    private Document setCreated(Document document) {
        document.setCreated(LocalDateTime.now().toString());
        document.setTimeoutEpoch(System.currentTimeMillis() + replyTimeout);
        return document;
    }

    private Mono<? extends ConsumerRecord<String, Document>> sendAndReceive(Document doc) {
        ProducerRecord<String, Document> producerRecord = new ProducerRecord<>(documentsTopic, UUID.randomUUID().toString(), doc);
        producerRecord.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, documentsTopic.getBytes()));
        return Mono.fromFuture(kafkaTemplate.sendAndReceive(producerRecord).completable());
    }

}
