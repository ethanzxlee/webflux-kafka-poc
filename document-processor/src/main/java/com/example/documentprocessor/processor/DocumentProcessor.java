package com.example.documentprocessor.processor;

import com.example.documentprocessor.model.Document;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashSet;

@Component
@Slf4j
public class DocumentProcessor {

    private static HashSet<String> processedDocs = new HashSet<>();

    @KafkaListener(topics = "${documents.topic}")
    @SendTo("${documents.topic}")
    public Document process(Document document) throws InterruptedException {
        log.info("RECEIVED: " + document.toString());

        if (processedDocs.contains(document.getId()) && StringUtils.equals(document.getStatus(), "ERROR")) {
            log.error("ERROR: rolling back " + document);
            return null;
        } else {
            document.setStatus("STEP-0-DONE");
            document.setContent(document.getContent() + "(processor)");
            document.setLastModified(LocalDateTime.now().toString());
            processedDocs.add(document.getId());
//            Thread.sleep(6000);
            return document;
        }
    }

}
