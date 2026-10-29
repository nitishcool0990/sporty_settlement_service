package com.sportygroup.settlement.service.impl;

import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.service.BetMatchingService;
import com.sportygroup.settlement.service.BetSettlementProducer;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.service.OutboxService;
import com.sportygroup.settlement.util.SafeMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class DefaultBetMatchingService implements BetMatchingService {

    private final BetRepository betRepository;
    private final BetSettlementProducer settlementProducer;
    private final CentralizedAuditService auditService;
    private final OutboxService outboxService;
    private final AppProperties appProperties;
    private final Executor matchingExecutor;

    public DefaultBetMatchingService(
            BetRepository betRepository,
            BetSettlementProducer settlementProducer,
            CentralizedAuditService auditService,
            OutboxService outboxService,
            AppProperties appProperties,
            @Qualifier("betMatchingExecutor") Executor matchingExecutor) {
        this.betRepository = betRepository;
        this.settlementProducer = settlementProducer;
        this.auditService = auditService;
        this.outboxService = outboxService;
        this.appProperties = appProperties;
        this.matchingExecutor = matchingExecutor;
    }

    @Override
    public CompletableFuture<Void> matchAndTriggerSettlementsAsync(EventOutcomeMessage outcomeMessage) {
        log.info("Submitting event outcome matching to dedicated thread pool for parallel execution: eventId={}",
                outcomeMessage.getEventId());
        return CompletableFuture.runAsync(() -> matchAndTriggerSettlements(outcomeMessage), matchingExecutor);
    }

    @Override
    @Transactional
    public void matchAndTriggerSettlements(EventOutcomeMessage outcomeMessage) {
        String eventId = outcomeMessage.getEventId();
        String actualWinnerId = outcomeMessage.getEventWinnerId();
        String traceId = outcomeMessage.getTraceId();

        if (eventId == null || eventId.isBlank()) {
            log.warn("Cannot match bets: eventId is null or empty in outcome message");
            return;
        }

        if (actualWinnerId == null || actualWinnerId.isBlank()) {
            log.warn("Cannot match bets: actualWinnerId is null or blank for eventId={}", eventId);
            auditService.record(
                    traceId,
                    "OUTCOME_INVALID_NO_WINNER",
                    eventId,
                    null,
                    null,
                    "Ignored outcome message: actualWinnerId is null or blank for event: " + eventId,
                    SafeMap.of("eventId", eventId, "error", "MISSING_WINNER_ID")
            );
            return;
        }

        log.info("[THREAD={}] Matching bets for eventId={} with actualWinnerId={}",
                Thread.currentThread().getName(), eventId, actualWinnerId);

        // Fetch all pending bets for this event
        List<Bet> pendingBets = betRepository.findByEventIdAndStatus(eventId, BetStatus.PENDING);

        if (pendingBets.isEmpty()) {
            log.warn("No pending bets found to settle for eventId={}", eventId);
            auditService.record(
                    traceId,
                    "BETS_MATCHED",
                    eventId,
                    null,
                    null,
                    "No pending bets found in database for event: " + eventId,
                    SafeMap.of("pendingCount", 0, "actualWinnerId", actualWinnerId)
            );
            return;
        }

        log.info("Found {} pending bets to settle for eventId={}", pendingBets.size(), eventId);

        auditService.record(
                traceId,
                "BETS_MATCHED",
                eventId,
                null,
                null,
                "Found " + pendingBets.size() + " pending bets to settle for event: " + eventId,
                SafeMap.of("pendingCount", pendingBets.size(), "actualWinnerId", actualWinnerId)
        );

        boolean useOutbox = appProperties.getOutbox().isEnabled();
        String targetTopic = appProperties.getRocketmq().getTopics().getBetSettlements();

        List<CompletableFuture<Void>> dispatchFutures = new ArrayList<>(pendingBets.size());

        for (Bet bet : pendingBets) {
            boolean isWinner = actualWinnerId.equalsIgnoreCase(bet.getEventWinnerId());
            BetOutcome outcome = isWinner ? BetOutcome.WON : BetOutcome.LOST;

            BigDecimal odds = (bet.getOdds() != null && bet.getOdds().compareTo(BigDecimal.ZERO) > 0)
                    ? bet.getOdds()
                    : BigDecimal.valueOf(2.00);

            BigDecimal payoutAmount = isWinner
                    ? bet.getBetAmount().multiply(odds).setScale(2, RoundingMode.HALF_UP)
                    : BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);

            BetSettlementMessage settlementMessage = BetSettlementMessage.builder()
                    .betId(bet.getBetId())
                    .userId(bet.getUserId())
                    .eventId(bet.getEventId())
                    .eventMarketId(bet.getEventMarketId())
                    .eventWinnerId(bet.getEventWinnerId())
                    .actualWinnerId(actualWinnerId)
                    .betAmount(bet.getBetAmount())
                    .odds(odds)
                    .payoutAmount(payoutAmount)
                    .outcome(outcome)
                    .traceId(traceId)
                    .dispatchedAt(Instant.now())
                    .build();

            if (useOutbox) {
                // Transactional Outbox Pattern: Save event to outbox within this ACID transaction
                outboxService.save("BET", bet.getBetId(), "BET_SETTLED", targetTopic, settlementMessage, traceId);
            } else {
                // Direct MQ dispatch fallback
                log.info("Triggering direct settlement dispatch: betId={}, predictedWinner={}, actualWinner={}, outcome={}, payout={}",
                        bet.getBetId(), bet.getEventWinnerId(), actualWinnerId, outcome, payoutAmount);
                CompletableFuture<Void> future = settlementProducer.sendSettlement(settlementMessage);
                dispatchFutures.add(future);
            }
        }

        if (useOutbox) {
            log.info("Successfully persisted {} settlements into Transactional Outbox for eventId={}",
                    pendingBets.size(), eventId);
            // Trigger outbox relay asynchronously to minimize latency
            outboxService.relayImmediatelyAsync();
        } else {
            // Wait for all direct settlement messages to be acknowledged by RocketMQ
            try {
                CompletableFuture.allOf(dispatchFutures.toArray(new CompletableFuture[0]))
                        .get(15, TimeUnit.SECONDS);
                log.info("All {} settlement tasks successfully acknowledged by RocketMQ for eventId={}",
                        dispatchFutures.size(), eventId);
            } catch (Exception ex) {
                log.error("Failed to publish all settlements to RocketMQ for eventId={}: {}", eventId, ex.getMessage(), ex);
                auditService.record(
                        traceId,
                        "SETTLEMENT_DISPATCH_FAILURE",
                        eventId,
                        null,
                        null,
                        "RocketMQ dispatch failed: " + ex.getMessage(),
                        SafeMap.of("error", ex.getMessage())
                );
                throw new RuntimeException("Settlement dispatch failed for event: " + eventId, ex);
            }
        }
    }
}
