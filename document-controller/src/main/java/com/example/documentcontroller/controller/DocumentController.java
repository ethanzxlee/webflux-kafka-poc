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
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
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
    public ResponseEntity<Mono<Document>> postDocument(Document document) {
        Mono<Document> documentMono = Mono.just(document)
            .map(this::initDocument)
            .flatMap(this::sendAndReceive)
            .map(ConsumerRecord::value)
            .onErrorMap(t -> sendErrorMessage(t, document));

        return new ResponseEntity<>(documentMono, HttpStatus.OK);
    }

    private Document initDocument(Document document) {
        document.setCreated(LocalDateTime.now().toString());
        document.setTimeoutEpoch(System.currentTimeMillis() + replyTimeout);
        document.setId(UUID.randomUUID().toString());
        return document;
    }

    private Mono<? extends ConsumerRecord<String, Document>> sendAndReceive(Document doc) {
        ProducerRecord<String, Document> producerRecord = new ProducerRecord<>(documentsTopic, doc.getId(), doc);
        producerRecord.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, documentsTopic.getBytes()));
        return Mono.fromFuture(kafkaTemplate.sendAndReceive(producerRecord).completable());
    }

    private Throwable sendErrorMessage(Throwable throwable, Document document) {
        document.setStatus("ERROR");

        if (throwable instanceof KafkaReplyTimeoutException) {
            ProducerRecord<String, Document> producerRecord = new ProducerRecord<>(documentsTopic, document.getId(), document);
            producerRecord.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, documentsTopic.getBytes()));
        }

        return throwable;
    }

}
