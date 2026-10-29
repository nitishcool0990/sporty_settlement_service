package com.sportygroup.settlement.domain.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "bets", indexes = {
        @Index(name = "idx_bets_event_id", columnList = "eventId"),
        @Index(name = "idx_bets_status", columnList = "status"),
        @Index(name = "idx_bets_user_id", columnList = "userId")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Bet {

    @Id
    @Column(nullable = false, unique = true, length = 64)
    private String betId;

    @Column(nullable = false, length = 64)
    private String userId;

    @Column(nullable = false, length = 64)
    private String eventId;

    @Column(nullable = false, length = 64)
    private String eventMarketId;

    @Column(nullable = false, length = 64)
    private String eventWinnerId;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal betAmount;

    @Column(precision = 6, scale = 2)
    @Builder.Default
    private BigDecimal odds = BigDecimal.valueOf(2.00);

    @Column(precision = 12, scale = 2)
    private BigDecimal payoutAmount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    @Builder.Default
    private BetStatus status = BetStatus.PENDING;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    @Builder.Default
    private BetOutcome outcome = BetOutcome.PENDING;

    @Version
    private Long version;

    @Column(nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    private Instant settledAt;

    @Column(length = 64)
    private String settlementTraceId;
}
