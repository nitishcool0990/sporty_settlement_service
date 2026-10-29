package com.sportygroup.settlement.service.impl;

import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.dto.request.EventOutcomeRequest;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.service.EventOutcomePublisher;
import com.sportygroup.settlement.util.SafeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventOutcomePublisher implements EventOutcomePublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppProperties appProperties;
    private final CentralizedAuditService auditService;

    @Override
    public CompletableFuture<EventOutcomeMessage> publishOutcome(EventOutcomeRequest request, String traceId) {
        String topic = appProperties.getKafka().getTopics().getEventOutcomes();

        EventOutcomeMessage message = EventOutcomeMessage.builder()
                .eventId(request.getEventId())
                .eventName(request.getEventName())
                .eventWinnerId(request.getEventWinnerId())
                .traceId(traceId)
                .publishedAt(Instant.now())
                .build();

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, request.getEventId(), message);
        record.headers().add(new RecordHeader("X-Correlation-Id", traceId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("X-Event-Id", request.getEventId().getBytes(StandardCharsets.UTF_8)));

        log.info("Publishing sports event outcome to Kafka topic [{}]: eventId={}, winnerId={}",
                topic, request.getEventId(), request.getEventWinnerId());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

        return future.thenApply(result -> {
            log.info("Successfully published event outcome to Kafka: topic={}, partition={}, offset={}",
                    topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

            auditService.record(
                    traceId,
                    "KAFKA_PUBLISHED",
                    request.getEventId(),
                    null,
                    null,
                    "Published event outcome to Kafka topic: " + topic,
                    SafeMap.of(
                            "topic", topic,
                            "partition", result.getRecordMetadata().partition(),
                            "offset", result.getRecordMetadata().offset(),
                            "winnerId", request.getEventWinnerId()
                    )
            );
            return message;
        }).exceptionally(ex -> {
            String errorMessage = ex != null && ex.getMessage() != null ? ex.getMessage() : "Unknown Kafka publish error";
            log.error("Failed to publish event outcome to Kafka topic [{}]: {}", topic, errorMessage, ex);
            auditService.record(
                    traceId,
                    "KAFKA_PUBLISH_FAILED",
                    request.getEventId(),
                    null,
                    null,
                    "Failed to publish event outcome to Kafka: " + errorMessage,
                    SafeMap.of("error", errorMessage)
            );
            throw new RuntimeException("Kafka publish failed: " + errorMessage, ex);
        });
    }
}
