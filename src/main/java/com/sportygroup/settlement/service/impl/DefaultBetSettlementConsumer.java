package com.sportygroup.settlement.service.impl;

import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.service.BetSettlementConsumer;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.util.SafeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultBetSettlementConsumer implements BetSettlementConsumer {

    private final BetRepository betRepository;
    private final CentralizedAuditService auditService;

    @Override
    @Transactional
    public void handleSettlement(BetSettlementMessage message) {
        if (message == null || message.getBetId() == null) {
            log.warn("Received empty or invalid settlement message");
            return;
        }

        try {
            if (message.getTraceId() != null) {
                MDC.put("traceId", message.getTraceId());
            }
            if (message.getEventId() != null) {
                MDC.put("eventId", message.getEventId());
            }
            MDC.put("betId", message.getBetId());
            if (message.getUserId() != null) {
                MDC.put("userId", message.getUserId());
            }

            log.info("[ROCKETMQ-CONSUMER] Processing settlement for betId={}, userId={}, eventId={}, outcome={}, payout={}",
                    message.getBetId(), message.getUserId(), message.getEventId(),
                    message.getOutcome(), message.getPayoutAmount());

            auditService.record(
                    message.getTraceId(),
                    "ROCKETMQ_CONSUMED",
                    message.getEventId(),
                    message.getBetId(),
                    message.getUserId(),
                    "Received settlement message from RocketMQ",
                    SafeMap.of(
                            "betId", message.getBetId(),
                            "outcome", message.getOutcome(),
                            "payout", message.getPayoutAmount()
                    )
            );

            // Conditional atomic update: Only update if the bet is currently in PENDING status
            int updatedRows = betRepository.updateBetSettlementIfStatus(
                    message.getBetId(),
                    BetStatus.PENDING,
                    BetStatus.SETTLED,
                    message.getOutcome(),
                    message.getPayoutAmount(),
                    Instant.now(),
                    message.getTraceId()
            );

            if (updatedRows > 0) {
                log.info("[BET-SETTLED] Bet {} successfully transitioned from PENDING to SETTLED: outcome={}, payout={}",
                        message.getBetId(), message.getOutcome(), message.getPayoutAmount());

                Runnable recordSettledAudit = () -> auditService.record(
                        message.getTraceId(),
                        "BET_SETTLED",
                        message.getEventId(),
                        message.getBetId(),
                        message.getUserId(),
                        "Bet settled successfully as " + message.getOutcome() + " with payout: " + message.getPayoutAmount(),
                        SafeMap.of(
                                "status", BetStatus.SETTLED,
                                "outcome", message.getOutcome(),
                                "payoutAmount", message.getPayoutAmount(),
                                "betAmount", message.getBetAmount(),
                                "odds", message.getOdds()
                        )
                );

                if (org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive()) {
                    org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization(
                            new org.springframework.transaction.support.TransactionSynchronization() {
                                @Override
                                public void afterCommit() {
                                    recordSettledAudit.run();
                                }
                            }
                    );
                } else {
                    recordSettledAudit.run();
                }
            } else {
                // Determine if bet does not exist, or was already settled
                Optional<Bet> existingBet = betRepository.findById(message.getBetId());
                if (existingBet.isEmpty()) {
                    log.error("Bet not found in database for settlement: betId={}", message.getBetId());
                    auditService.record(
                            message.getTraceId(),
                            "BET_SETTLEMENT_FAILED",
                            message.getEventId(),
                            message.getBetId(),
                            message.getUserId(),
                            "Bet not found in database: " + message.getBetId(),
                            SafeMap.of("error", "NOT_FOUND")
                    );
                } else {
                    Bet bet = existingBet.get();
                    log.warn("[IDEMPOTENCY] Bet {} is already in status '{}' with outcome '{}'. Skipping duplicate settlement.",
                            bet.getBetId(), bet.getStatus(), bet.getOutcome());

                    auditService.record(
                            message.getTraceId(),
                            "BET_ALREADY_SETTLED_SKIPPED",
                            message.getEventId(),
                            message.getBetId(),
                            message.getUserId(),
                            "Duplicate settlement skipped. Bet " + bet.getBetId() + " already settled as " + bet.getOutcome(),
                            SafeMap.of(
                                    "currentStatus", bet.getStatus(),
                                    "currentOutcome", bet.getOutcome(),
                                    "skippedOutcome", message.getOutcome()
                            )
                    );
                }
            }
        } finally {
            MDC.clear();
        }
    }
}
