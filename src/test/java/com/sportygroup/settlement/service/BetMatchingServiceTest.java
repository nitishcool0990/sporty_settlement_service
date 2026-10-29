package com.sportygroup.settlement.service;

import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.service.impl.DefaultBetMatchingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BetMatchingServiceTest {

    @Mock
    private BetRepository betRepository;

    @Mock
    private BetSettlementProducer settlementProducer;

    @Mock
    private CentralizedAuditService auditService;

    @Mock
    private OutboxService outboxService;

    @Spy
    private AppProperties appProperties = new AppProperties();

    @Mock
    private java.util.concurrent.Executor matchingExecutor;

    @InjectMocks
    private DefaultBetMatchingService matchingService;

    private EventOutcomeMessage outcomeMessage;

    @BeforeEach
    void setUp() {
        outcomeMessage = EventOutcomeMessage.builder()
                .eventId("EVT-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId("TEAM-ARS")
                .traceId("trace-123")
                .publishedAt(Instant.now())
                .build();
    }

    @Test
    @DisplayName("Should persist calculated settlement into Transactional Outbox when outbox is enabled")
    void shouldPersistToOutboxWhenOutboxEnabled() {
        appProperties.getOutbox().setEnabled(true);

        Bet winningBet = Bet.builder()
                .betId("BET-1")
                .userId("USR-1")
                .eventId("EVT-CHEL-ARS")
                .eventMarketId("MKT-1")
                .eventWinnerId("TEAM-ARS")
                .betAmount(BigDecimal.valueOf(100.00))
                .odds(BigDecimal.valueOf(2.50))
                .status(BetStatus.PENDING)
                .outcome(BetOutcome.PENDING)
                .build();

        Bet losingBet = Bet.builder()
                .betId("BET-2")
                .userId("USR-2")
                .eventId("EVT-CHEL-ARS")
                .eventMarketId("MKT-1")
                .eventWinnerId("TEAM-CHEL")
                .betAmount(BigDecimal.valueOf(50.00))
                .odds(BigDecimal.valueOf(1.90))
                .status(BetStatus.PENDING)
                .outcome(BetOutcome.PENDING)
                .build();

        when(betRepository.findByEventIdAndStatus("EVT-CHEL-ARS", BetStatus.PENDING))
                .thenReturn(List.of(winningBet, losingBet));

        matchingService.matchAndTriggerSettlements(outcomeMessage);

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(outboxService, times(2)).save(
                eq("BET"),
                any(),
                eq("BET_SETTLED"),
                eq("bet-settlements"),
                payloadCaptor.capture(),
                eq("trace-123")
        );

        List<Object> capturedPayloads = payloadCaptor.getAllValues();
        BetSettlementMessage winSettlement = (BetSettlementMessage) capturedPayloads.get(0);
        assertThat(winSettlement.getBetId()).isEqualTo("BET-1");
        assertThat(winSettlement.getOutcome()).isEqualTo(BetOutcome.WON);
        assertThat(winSettlement.getPayoutAmount()).isEqualTo(BigDecimal.valueOf(250.00).setScale(2, RoundingMode.HALF_UP));

        BetSettlementMessage lossSettlement = (BetSettlementMessage) capturedPayloads.get(1);
        assertThat(lossSettlement.getBetId()).isEqualTo("BET-2");
        assertThat(lossSettlement.getOutcome()).isEqualTo(BetOutcome.LOST);
        assertThat(lossSettlement.getPayoutAmount()).isEqualTo(BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP));

        verify(outboxService).relayImmediatelyAsync();
        verify(settlementProducer, never()).sendSettlement(any());
    }

    @Test
    @DisplayName("Should dispatch directly to RocketMQ when outbox is disabled (fallback mode)")
    void shouldDispatchDirectlyWhenOutboxDisabled() {
        appProperties.getOutbox().setEnabled(false);

        Bet winningBet = Bet.builder()
                .betId("BET-1")
                .userId("USR-1")
                .eventId("EVT-CHEL-ARS")
                .eventMarketId("MKT-1")
                .eventWinnerId("TEAM-ARS")
                .betAmount(BigDecimal.valueOf(100.00))
                .odds(BigDecimal.valueOf(2.50))
                .status(BetStatus.PENDING)
                .outcome(BetOutcome.PENDING)
                .build();

        when(betRepository.findByEventIdAndStatus("EVT-CHEL-ARS", BetStatus.PENDING))
                .thenReturn(List.of(winningBet));
        when(settlementProducer.sendSettlement(any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        matchingService.matchAndTriggerSettlements(outcomeMessage);

        ArgumentCaptor<BetSettlementMessage> captor = ArgumentCaptor.forClass(BetSettlementMessage.class);
        verify(settlementProducer).sendSettlement(captor.capture());

        BetSettlementMessage captured = captor.getValue();
        assertThat(captured.getBetId()).isEqualTo("BET-1");
        assertThat(captured.getOutcome()).isEqualTo(BetOutcome.WON);
        assertThat(captured.getPayoutAmount()).isEqualTo(BigDecimal.valueOf(250.00).setScale(2, RoundingMode.HALF_UP));

        verify(outboxService, never()).save(any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("Should throw exception if direct RocketMQ dispatch fails when outbox is disabled")
    void shouldThrowIfRocketMqDispatchFails() {
        appProperties.getOutbox().setEnabled(false);

        Bet bet = Bet.builder()
                .betId("BET-1")
                .userId("USR-1")
                .eventId("EVT-CHEL-ARS")
                .eventMarketId("MKT-1")
                .eventWinnerId("TEAM-ARS")
                .betAmount(BigDecimal.valueOf(100.00))
                .odds(BigDecimal.valueOf(2.50))
                .status(BetStatus.PENDING)
                .outcome(BetOutcome.PENDING)
                .build();

        when(betRepository.findByEventIdAndStatus("EVT-CHEL-ARS", BetStatus.PENDING))
                .thenReturn(List.of(bet));
        when(settlementProducer.sendSettlement(any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("RocketMQ Broker unavailable")));

        assertThatThrownBy(() -> matchingService.matchAndTriggerSettlements(outcomeMessage))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Settlement dispatch failed");
    }

    @Test
    @DisplayName("Should reject and ignore outcome with null or blank actualWinnerId, leaving bets untouched")
    void shouldIgnoreOutcomeWhenWinnerIdIsMissingOrBlank() {
        EventOutcomeMessage outcomeWithoutWinner = EventOutcomeMessage.builder()
                .eventId("EVT-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId(null) // missing winner
                .traceId("trace-no-winner")
                .publishedAt(Instant.now())
                .build();

        matchingService.matchAndTriggerSettlements(outcomeWithoutWinner);

        // Crucial: No bets should ever be queried or dispatched to be settled as LOST!
        verify(betRepository, never()).findByEventIdAndStatus(any(), any());
        verify(settlementProducer, never()).sendSettlement(any());
        verify(auditService).record(eq("trace-no-winner"), eq("OUTCOME_INVALID_NO_WINNER"), eq("EVT-CHEL-ARS"), any(), any(), any(), any());
    }

    @Test
    @DisplayName("Should submit matching task to dedicated thread pool for parallel execution")
    void shouldExecuteMatchingAsynchronouslyInSeparateThreadPool() {
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(matchingExecutor).execute(any(Runnable.class));

        when(betRepository.findByEventIdAndStatus("EVT-CHEL-ARS", BetStatus.PENDING))
                .thenReturn(Collections.emptyList());

        CompletableFuture<Void> future = matchingService.matchAndTriggerSettlementsAsync(outcomeMessage);

        assertThat(future).isNotNull();
        future.join();
        verify(matchingExecutor).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("Should handle scenario where no bets exist for the event gracefully")
    void shouldHandleNoBetsGracefully() {
        when(betRepository.findByEventIdAndStatus("EVT-CHEL-ARS", BetStatus.PENDING))
                .thenReturn(Collections.emptyList());

        matchingService.matchAndTriggerSettlements(outcomeMessage);

        verify(settlementProducer, never()).sendSettlement(any());
        verify(auditService).record(eq("trace-123"), eq("BETS_MATCHED"), eq("EVT-CHEL-ARS"), any(), any(), any(), any());
    }
}
