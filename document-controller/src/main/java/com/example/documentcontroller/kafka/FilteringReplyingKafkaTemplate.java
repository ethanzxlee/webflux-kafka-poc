package com.example.documentcontroller.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A replying template that filters replies
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 */
public class FilteringReplyingKafkaTemplate<K, V, R>
        extends ReplyingKafkaTemplate<K, V, R> {

    private Predicate<ConsumerRecord<K, R>> filter = consumerRecord -> false;

    public FilteringReplyingKafkaTemplate(
            ProducerFactory<K, V> producerFactory,
            GenericMessageListenerContainer<K, R> replyContainer) {
        super(producerFactory, replyContainer);
    }

    public FilteringReplyingKafkaTemplate(
            ProducerFactory<K, V> producerFactory,
            GenericMessageListenerContainer<K, R> replyContainer,
            boolean autoFlush) {
        super(producerFactory, replyContainer, autoFlush);
    }

    public FilteringReplyingKafkaTemplate(
            ProducerFactory<K, V> producerFactory,
            GenericMessageListenerContainer<K, R> replyContainer,
            Predicate<ConsumerRecord<K, R>> filter) {
        super(producerFactory, replyContainer);
        this.filter = filter;
    }

    @Override
    public void onMessage(List<ConsumerRecord<K, R>> data) {
        super.onMessage(data.stream().filter(filter).collect(Collectors.toList()));
    }
}
