package com.example.documentprocessor.processor;

import com.example.documentprocessor.model.Document;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class DocumentProcessor {
    @KafkaListener(topics = "${documents.topic}")
    @SendTo
    public Document process(Document document) {
        document.setStatus("DONE");
        document.setLastModified(LocalDateTime.now().toString());
        return document;
    }
}
