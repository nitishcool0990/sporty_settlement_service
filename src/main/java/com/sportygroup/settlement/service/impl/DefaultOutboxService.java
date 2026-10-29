package com.sportygroup.settlement.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.OutboxMessage;
import com.sportygroup.settlement.domain.model.OutboxStatus;
import com.sportygroup.settlement.domain.repository.OutboxMessageRepository;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.service.BetSettlementProducer;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.service.OutboxService;
import com.sportygroup.settlement.util.SafeMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class DefaultOutboxService implements OutboxService {

    private final OutboxMessageRepository outboxRepository;
    private final BetSettlementProducer settlementProducer;
    private final CentralizedAuditService auditService;
    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;
    private final Executor matchingExecutor;

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);

    public DefaultOutboxService(
            OutboxMessageRepository outboxRepository,
            BetSettlementProducer settlementProducer,
            CentralizedAuditService auditService,
            ObjectMapper objectMapper,
            AppProperties appProperties,
            @Qualifier("betMatchingExecutor") Executor matchingExecutor) {
        this.outboxRepository = outboxRepository;
        this.settlementProducer = settlementProducer;
        this.auditService = auditService;
        this.objectMapper = objectMapper;
        this.appProperties = appProperties;
        this.matchingExecutor = matchingExecutor;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public OutboxMessage save(String aggregateType, String aggregateId, String eventType, String topic, Object payload, String traceId) {
        String payloadJson;
        try {
            payloadJson = (payload instanceof String s) ? s : objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize outbox message payload for aggregateId={}: {}", aggregateId, e.getMessage(), e);
            throw new IllegalArgumentException("Could not serialize outbox payload", e);
        }

        OutboxMessage outboxMessage = OutboxMessage.builder()
                .id(UUID.randomUUID().toString())
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .topic(topic)
                .payload(payloadJson)
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .traceId(traceId)
                .createdAt(Instant.now())
                .build();

        OutboxMessage saved = outboxRepository.save(outboxMessage);
        log.info("[OUTBOX-SAVED] Persisted outbox message id={} for aggregateId={}, eventType={}, topic={}",
                saved.getId(), aggregateId, eventType, topic);

        auditService.record(
                traceId,
                "OUTBOX_MESSAGE_SAVED",
                null,
                aggregateId,
                null,
                "Outbox message queued for transactional dispatch to topic: " + topic,
                SafeMap.of(
                        "outboxId", saved.getId(),
                        "aggregateId", aggregateId,
                        "eventType", eventType,
                        "topic", topic
                )
        );

        return saved;
    }

    @Override
    public int processPendingMessages() {
        if (!appProperties.getOutbox().isEnabled()) {
            return 0;
        }

        // Prevent overlapping polling executions
        if (!isProcessing.compareAndSet(false, true)) {
            log.debug("Outbox relay is already in progress, skipping concurrent poll");
            return 0;
        }

        try {
            int batchSize = appProperties.getOutbox().getBatchSize();
            int maxRetries = appProperties.getOutbox().getMaxRetries();

            List<OutboxMessage> pendingMessages = outboxRepository.findByStatusOrderByCreatedAtAsc(
                    OutboxStatus.PENDING, PageRequest.of(0, batchSize));

            if (pendingMessages.isEmpty()) {
                return 0;
            }

            log.info("[OUTBOX-RELAY] Polled {} pending messages for publishing", pendingMessages.size());

            int publishedCount = 0;
            for (OutboxMessage outbox : pendingMessages) {
                boolean published = dispatchSingleMessage(outbox, maxRetries);
                if (published) {
                    publishedCount++;
                }
            }

            return publishedCount;
        } finally {
            isProcessing.set(false);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean dispatchSingleMessage(OutboxMessage outbox, int maxRetries) {
        String outboxId = outbox.getId();
        String traceId = outbox.getTraceId();

        try {
            BetSettlementMessage settlement = objectMapper.readValue(outbox.getPayload(), BetSettlementMessage.class);

            log.info("[OUTBOX-RELAY-DISPATCH] Dispatching outbox id={} to RocketMQ: betId={}, traceId={}",
                    outboxId, settlement.getBetId(), traceId);

            CompletableFuture<Void> sendFuture = settlementProducer.sendSettlement(settlement);
            sendFuture.get(10, TimeUnit.SECONDS);

            outbox.setStatus(OutboxStatus.PUBLISHED);
            outbox.setProcessedAt(Instant.now());
            outbox.setErrorMessage(null);
            outboxRepository.save(outbox);

            log.info("[OUTBOX-PUBLISHED] Successfully dispatched outbox id={} to RocketMQ", outboxId);

            auditService.record(
                    traceId,
                    "OUTBOX_PUBLISHED",
                    settlement.getEventId(),
                    settlement.getBetId(),
                    settlement.getUserId(),
                    "Outbox message successfully delivered to RocketMQ topic: " + outbox.getTopic(),
                    SafeMap.of("outboxId", outboxId, "betId", settlement.getBetId())
            );

            return true;
        } catch (Exception ex) {
            String errorMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            int currentRetries = outbox.getRetryCount() + 1;
            outbox.setRetryCount(currentRetries);
            outbox.setErrorMessage(errorMsg.length() > 1000 ? errorMsg.substring(0, 1000) : errorMsg);

            if (currentRetries >= maxRetries) {
                outbox.setStatus(OutboxStatus.FAILED);
                log.error("[OUTBOX-DEAD-LETTER] Outbox id={} exceeded max retries ({}/{}). Marked as FAILED: {}",
                        outboxId, currentRetries, maxRetries, errorMsg);

                auditService.record(
                        traceId,
                        "OUTBOX_DEAD_LETTER",
                        null,
                        outbox.getAggregateId(),
                        null,
                        "Outbox message exceeded max retries and moved to dead-letter FAILED status: " + errorMsg,
                        SafeMap.of("outboxId", outboxId, "retries", currentRetries, "error", errorMsg)
                );
            } else {
                log.warn("[OUTBOX-RETRY] Failed to dispatch outbox id={} (attempt {}/{}): {}",
                        outboxId, currentRetries, maxRetries, errorMsg);

                auditService.record(
                        traceId,
                        "OUTBOX_DISPATCH_RETRY",
                        null,
                        outbox.getAggregateId(),
                        null,
                        "Outbox dispatch attempt failed, will retry: " + errorMsg,
                        SafeMap.of("outboxId", outboxId, "retries", currentRetries, "error", errorMsg)
                );
            }

            outboxRepository.save(outbox);
            return false;
        }
    }

    @Override
    public void relayImmediatelyAsync() {
        if (!appProperties.getOutbox().isEnabled()) {
            return;
        }
        CompletableFuture.runAsync(this::processPendingMessages, matchingExecutor)
                .exceptionally(ex -> {
                    log.warn("[OUTBOX-ASYNC] Async outbox relay encountered error: {}", ex.getMessage());
                    return null;
                });
    }

    @Override
    public List<OutboxMessage> getPendingMessages() {
        return outboxRepository.findByStatus(OutboxStatus.PENDING);
    }

    @Override
    public long countPendingMessages() {
        return outboxRepository.findByStatus(OutboxStatus.PENDING).size();
    }

    @Override
    @Transactional
    public int retryFailedMessages() {
        List<OutboxMessage> failed = outboxRepository.findByStatus(OutboxStatus.FAILED);
        for (OutboxMessage message : failed) {
            message.setStatus(OutboxStatus.PENDING);
            message.setRetryCount(0);
            message.setErrorMessage(null);
            outboxRepository.save(message);
        }
        log.info("[OUTBOX-RESET] Reset {} FAILED outbox messages back to PENDING for re-dispatch", failed.size());
        relayImmediatelyAsync();
        return failed.size();
    }
}
