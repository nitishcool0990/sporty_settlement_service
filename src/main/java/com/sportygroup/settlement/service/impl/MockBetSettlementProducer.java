package com.sportygroup.settlement.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.service.BetSettlementConsumer;
import com.sportygroup.settlement.service.BetSettlementProducer;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.util.SafeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.rocketmq.enabled", havingValue = "false", matchIfMissing = true)
public class MockBetSettlementProducer implements BetSettlementProducer {

    private final AppProperties appProperties;
    private final CentralizedAuditService auditService;
    private final ObjectMapper objectMapper;
    private final BetSettlementConsumer settlementConsumer;

    @Override
    public CompletableFuture<Void> sendSettlement(BetSettlementMessage settlementMessage) {
        String topic = appProperties.getRocketmq().getTopics().getBetSettlements();
        try {
            String jsonPayload = objectMapper.writeValueAsString(settlementMessage);
            log.info("[MOCK-ROCKETMQ-PRODUCER] Dispatched settlement to topic [{}]:\n{}", topic, jsonPayload);

            auditService.record(
                    settlementMessage.getTraceId(),
                    "ROCKETMQ_PUBLISHED",
                    settlementMessage.getEventId(),
                    settlementMessage.getBetId(),
                    settlementMessage.getUserId(),
                    "Dispatched bet settlement via Mock RocketMQ to topic: " + topic,
                    SafeMap.of(
                            "mode", "MOCK",
                            "topic", topic,
                            "betId", settlementMessage.getBetId(),
                            "outcome", settlementMessage.getOutcome(),
                            "payout", settlementMessage.getPayoutAmount()
                    )
            );

            // Asynchronously deliver to consumer and complete future once consumer executes
            return CompletableFuture.runAsync(() -> settlementConsumer.handleSettlement(settlementMessage));
        } catch (Exception ex) {
            log.error("[MOCK-ROCKETMQ-PRODUCER] Error serializing settlement payload: {}", ex.getMessage(), ex);
            return CompletableFuture.failedFuture(ex);
        }
    }
}
