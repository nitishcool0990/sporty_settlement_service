package com.sportygroup.settlement.listener;

import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.service.BetMatchingService;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.util.SafeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventOutcomeListener {

    private final BetMatchingService betMatchingService;
    private final CentralizedAuditService auditService;

    @KafkaListener(
            topics = "${app.kafka.topics.event-outcomes}",
            groupId = "${spring.kafka.consumer.group-id:sporty-settlement-group}",
            concurrency = "${app.kafka.concurrency:3}"
    )
    public void onEventOutcome(ConsumerRecord<String, EventOutcomeMessage> record) {
        EventOutcomeMessage message = record.value();
        String traceId = extractHeader(record, "X-Correlation-Id");
        if (traceId == null || traceId.isBlank()) {
            traceId = (message != null && message.getTraceId() != null) ? message.getTraceId() : UUID.randomUUID().toString();
        }

        String eventId = message != null ? message.getEventId() : extractHeader(record, "X-Event-Id");

        try {
            MDC.put("traceId", traceId);
            if (eventId != null) {
                MDC.put("eventId", eventId);
            }

            log.info("[KAFKA-CONSUMER] Received event outcome from topic [{}]: partition={}, offset={}, eventId={}, winnerId={}",
                    record.topic(), record.partition(), record.offset(),
                    eventId, (message != null ? message.getEventWinnerId() : "null"));

            auditService.record(
                    traceId,
                    "KAFKA_CONSUMED",
                    eventId,
                    null,
                    null,
                    "Received event outcome message from Kafka topic: " + record.topic(),
                    SafeMap.of(
                            "topic", record.topic(),
                            "partition", record.partition(),
                            "offset", record.offset(),
                            "winnerId", (message != null ? message.getEventWinnerId() : null),
                            "eventName", (message != null ? message.getEventName() : null)
                    )
            );

            if (message == null) {
                log.warn("[KAFKA-CONSUMER] Skipping empty/tombstone record at partition={}, offset={}",
                        record.partition(), record.offset());
                return;
            }

            // Execute bet matching in a separate thread from dedicated thread pool.
            // This frees up the Kafka consumer thread immediately so subsequent events can process concurrently in parallel.
            CompletableFuture<Void> asyncMatching = betMatchingService.matchAndTriggerSettlementsAsync(message);

            asyncMatching.exceptionally(ex -> {
                String errorMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
                log.error("[PARALLEL-MATCHER] Asynchronous matching failed for eventId={}: {}", eventId, errorMsg, ex);
                auditService.record(
                        message.getTraceId(),
                        "ASYNC_MATCHING_ERROR",
                        eventId,
                        null,
                        null,
                        "Async matching failed: " + errorMsg,
                        SafeMap.of("error", errorMsg)
                );
                return null;
            });

        } catch (Exception ex) {
            String errorMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            log.error("[KAFKA-CONSUMER] Error dispatching event outcome for eventId={}: {}", eventId, errorMsg, ex);
            auditService.record(
                    traceId,
                    "KAFKA_PROCESSING_ERROR",
                    eventId,
                    null,
                    null,
                    "Failed to process event outcome: " + errorMsg,
                    SafeMap.of("error", errorMsg)
            );
            throw ex;
        } finally {
            MDC.clear();
        }
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }
}
