package com.example.documentprocessor1.processor;

import com.example.documentprocessor1.model.Document;
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
        log.info("RECEIVED: " + document.toString());
        document.setStatus("DONE");
        document.setLastModified(LocalDateTime.now().toString());
        document.setContent(document.getContent() + "(processor1)");
        return document;
    }
}
