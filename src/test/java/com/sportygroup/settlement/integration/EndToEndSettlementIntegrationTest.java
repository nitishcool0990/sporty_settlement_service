package com.sportygroup.settlement.integration;

import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import com.sportygroup.settlement.dto.request.EventOutcomeRequest;
import com.sportygroup.settlement.service.EventOutcomePublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"event-outcomes"}, brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
})
class EndToEndSettlementIntegrationTest {

    @Autowired
    private EventOutcomePublisher publisher;

    @Autowired
    private BetRepository betRepository;

    @Autowired
    private com.sportygroup.settlement.domain.repository.OutboxMessageRepository outboxRepository;

    @Autowired
    private com.sportygroup.settlement.init.SampleDataInitializer sampleDataInitializer;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        if (betRepository.count() == 0) {
            sampleDataInitializer.run();
        }
    }

    @Test
    @DisplayName("End-to-End: Publish event outcome to Kafka -> Match bets -> RocketMQ -> Settle in DB, with Idempotency")
    void testFullEndToEndSettlementPipelineAndIdempotency() throws Exception {
        // Given bets seeded by SampleDataInitializer for event EVT-2026-CHEL-ARS:
        // BET-1001: TEAM-ARS, $100 @ 2.20
        // BET-1002: TEAM-CHEL, $50 @ 1.85
        // BET-1003: TEAM-ARS, $200 @ 2.20

        List<Bet> initialBets = betRepository.findByEventIdAndStatus("EVT-2026-CHEL-ARS", BetStatus.PENDING);
        assertThat(initialBets).hasSize(3);

        EventOutcomeRequest request = EventOutcomeRequest.builder()
                .eventId("EVT-2026-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId("TEAM-ARS")
                .build();

        // When: outcome is published to Kafka
        publisher.publishOutcome(request, "e2e-test-trace-1").get(5, TimeUnit.SECONDS);

        // Then: wait for Kafka consumer -> Matching -> RocketMQ Mock -> DB settlement
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Bet bet1001 = betRepository.findById("BET-1001").orElseThrow();
            Bet bet1002 = betRepository.findById("BET-1002").orElseThrow();
            Bet bet1003 = betRepository.findById("BET-1003").orElseThrow();

            // BET-1001 bet on TEAM-ARS -> SETTLED, WON, Payout = 100 * 2.20 = 220.00
            assertThat(bet1001.getStatus()).isEqualTo(BetStatus.SETTLED);
            assertThat(bet1001.getOutcome()).isEqualTo(BetOutcome.WON);
            assertThat(bet1001.getPayoutAmount()).isEqualByComparingTo(BigDecimal.valueOf(220.00));
            assertThat(bet1001.getSettledAt()).isNotNull();

            // BET-1002 bet on TEAM-CHEL -> SETTLED, LOST, Payout = 0.00
            assertThat(bet1002.getStatus()).isEqualTo(BetStatus.SETTLED);
            assertThat(bet1002.getOutcome()).isEqualTo(BetOutcome.LOST);
            assertThat(bet1002.getPayoutAmount()).isEqualByComparingTo(BigDecimal.ZERO);
            assertThat(bet1002.getSettledAt()).isNotNull();

            // BET-1003 bet on TEAM-ARS -> SETTLED, WON, Payout = 200 * 2.20 = 440.00
            assertThat(bet1003.getStatus()).isEqualTo(BetStatus.SETTLED);
            assertThat(bet1003.getOutcome()).isEqualTo(BetOutcome.WON);
            assertThat(bet1003.getPayoutAmount()).isEqualByComparingTo(BigDecimal.valueOf(440.00));
            assertThat(bet1003.getSettledAt()).isNotNull();

            // Transactional Outbox verification: All 3 bet settlement messages reached PUBLISHED status
            List<com.sportygroup.settlement.domain.model.OutboxMessage> publishedOutbox =
                    outboxRepository.findByStatus(com.sportygroup.settlement.domain.model.OutboxStatus.PUBLISHED);
            assertThat(publishedOutbox).hasSize(3);
        });

        // Test Idempotency: Publish a "conflicting" or duplicate event outcome for the same event
        Instant originalSettledTime = betRepository.findById("BET-1001").orElseThrow().getSettledAt();
        EventOutcomeRequest duplicateRequest = EventOutcomeRequest.builder()
                .eventId("EVT-2026-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId("TEAM-CHEL") // Attempt to flip outcome
                .build();

        publisher.publishOutcome(duplicateRequest, "e2e-test-trace-2").get(5, TimeUnit.SECONDS);

        // Sleep briefly to allow any rogue processing
        Thread.sleep(1500);

        // Verify: Bets are STILL settled with their original outcome; NOT flipped or changed
        Bet bet1001After = betRepository.findById("BET-1001").orElseThrow();
        assertThat(bet1001After.getStatus()).isEqualTo(BetStatus.SETTLED);
        assertThat(bet1001After.getOutcome()).isEqualTo(BetOutcome.WON);
        assertThat(bet1001After.getPayoutAmount()).isEqualByComparingTo(BigDecimal.valueOf(220.00));
        assertThat(bet1001After.getSettledAt()).isEqualTo(originalSettledTime);
    }
}
