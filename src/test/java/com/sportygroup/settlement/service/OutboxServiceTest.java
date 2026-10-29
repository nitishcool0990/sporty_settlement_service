package com.sportygroup.settlement.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.OutboxMessage;
import com.sportygroup.settlement.domain.model.OutboxStatus;
import com.sportygroup.settlement.domain.repository.OutboxMessageRepository;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.service.impl.DefaultOutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {

    @Mock
    private OutboxMessageRepository outboxRepository;

    @Mock
    private BetSettlementProducer settlementProducer;

    @Mock
    private CentralizedAuditService auditService;

    @Mock
    private Executor matchingExecutor;

    private ObjectMapper objectMapper;
    private AppProperties appProperties;
    private DefaultOutboxService outboxService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        appProperties = new AppProperties();
        appProperties.getOutbox().setEnabled(true);
        appProperties.getOutbox().setBatchSize(10);
        appProperties.getOutbox().setMaxRetries(3);

        outboxService = new DefaultOutboxService(
                outboxRepository,
                settlementProducer,
                auditService,
                objectMapper,
                appProperties,
                matchingExecutor
        );
    }

    @Test
    @DisplayName("Should persist outbox message with PENDING status and audit the creation")
    void shouldPersistOutboxMessageCorrectly() {
        BetSettlementMessage payload = BetSettlementMessage.builder()
                .betId("BET-101")
                .eventId("EVT-1")
                .traceId("trace-xyz")
                .dispatchedAt(Instant.now())
                .build();

        when(outboxRepository.save(any(OutboxMessage.class))).thenAnswer(invocation -> invocation.getArgument(0));

        OutboxMessage saved = outboxService.save(
                "BET", "BET-101", "BET_SETTLED", "bet-settlements", payload, "trace-xyz");

        assertThat(saved).isNotNull();
        assertThat(saved.getAggregateType()).isEqualTo("BET");
        assertThat(saved.getAggregateId()).isEqualTo("BET-101");
        assertThat(saved.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(saved.getPayload()).contains("BET-101");
        assertThat(saved.getRetryCount()).isEqualTo(0);

        verify(outboxRepository).save(any(OutboxMessage.class));
        verify(auditService).record(eq("trace-xyz"), eq("OUTBOX_MESSAGE_SAVED"), any(), eq("BET-101"), any(), any(), any());
    }

    @Test
    @DisplayName("Should poll pending outbox messages, dispatch to RocketMQ, and mark as PUBLISHED")
    void shouldProcessPendingMessagesSuccessfully() {
        OutboxMessage message = OutboxMessage.builder()
                .id("outbox-1")
                .aggregateType("BET")
                .aggregateId("BET-101")
                .eventType("BET_SETTLED")
                .topic("bet-settlements")
                .payload("{\"betId\":\"BET-101\",\"eventId\":\"EVT-1\",\"traceId\":\"trace-abc\"}")
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .traceId("trace-abc")
                .createdAt(Instant.now())
                .build();

        when(outboxRepository.findByStatusOrderByCreatedAtAsc(eq(OutboxStatus.PENDING), any(Pageable.class)))
                .thenReturn(List.of(message));
        when(settlementProducer.sendSettlement(any(BetSettlementMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(outboxRepository.save(any(OutboxMessage.class))).thenAnswer(inv -> inv.getArgument(0));

        int processed = outboxService.processPendingMessages();

        assertThat(processed).isEqualTo(1);
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(message.getProcessedAt()).isNotNull();

        verify(settlementProducer).sendSettlement(any(BetSettlementMessage.class));
        verify(auditService).record(eq("trace-abc"), eq("OUTBOX_PUBLISHED"), eq("EVT-1"), eq("BET-101"), any(), any(), any());
    }

    @Test
    @DisplayName("Should increment retryCount when dispatch fails, and mark as FAILED after max retries")
    void shouldHandleDispatchFailureAndDeadLetter() {
        OutboxMessage message = OutboxMessage.builder()
                .id("outbox-2")
                .aggregateType("BET")
                .aggregateId("BET-102")
                .eventType("BET_SETTLED")
                .topic("bet-settlements")
                .payload("{\"betId\":\"BET-102\",\"eventId\":\"EVT-2\",\"traceId\":\"trace-fail\"}")
                .status(OutboxStatus.PENDING)
                .retryCount(2) // 2 retries already, next will reach maxRetries=3
                .traceId("trace-fail")
                .createdAt(Instant.now())
                .build();

        when(settlementProducer.sendSettlement(any(BetSettlementMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("RocketMQ connection refused")));
        when(outboxRepository.save(any(OutboxMessage.class))).thenAnswer(inv -> inv.getArgument(0));

        boolean result = outboxService.dispatchSingleMessage(message, 3);

        assertThat(result).isFalse();
        assertThat(message.getRetryCount()).isEqualTo(3);
        assertThat(message.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(message.getErrorMessage()).contains("RocketMQ connection refused");

        verify(auditService).record(eq("trace-fail"), eq("OUTBOX_DEAD_LETTER"), any(), eq("BET-102"), any(), any(), any());
    }

    @Test
    @DisplayName("Should re-arm FAILED dead-letter messages back to PENDING when retryFailedMessages is called")
    void shouldRearmFailedMessages() {
        OutboxMessage failedMessage = OutboxMessage.builder()
                .id("outbox-3")
                .aggregateType("BET")
                .aggregateId("BET-103")
                .status(OutboxStatus.FAILED)
                .retryCount(3)
                .errorMessage("Broker timeout")
                .build();

        when(outboxRepository.findByStatus(OutboxStatus.FAILED)).thenReturn(List.of(failedMessage));

        int rearmed = outboxService.retryFailedMessages();

        assertThat(rearmed).isEqualTo(1);
        assertThat(failedMessage.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(failedMessage.getRetryCount()).isEqualTo(0);
        assertThat(failedMessage.getErrorMessage()).isNull();

        verify(outboxRepository).save(failedMessage);
    }
}
