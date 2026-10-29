package com.sportygroup.settlement.scheduler;

import com.sportygroup.settlement.service.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background scheduler that periodically polls the transactional outbox table
 * and publishes pending domain events to the message broker (RocketMQ).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelayScheduler {

    private final OutboxService outboxService;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    public void relayPendingOutboxMessages() {
        try {
            int dispatched = outboxService.processPendingMessages();
            if (dispatched > 0) {
                log.info("[OUTBOX-SCHEDULER] Successfully dispatched {} outbox messages in this cycle", dispatched);
            }
        } catch (Exception ex) {
            log.error("[OUTBOX-SCHEDULER] Error during scheduled outbox relay cycle: {}", ex.getMessage(), ex);
        }
    }
}
