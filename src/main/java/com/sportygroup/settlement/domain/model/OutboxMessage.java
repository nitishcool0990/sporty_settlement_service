package com.sportygroup.settlement.domain.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "outbox_messages", indexes = {
        @Index(name = "idx_outbox_status_created", columnList = "status, createdAt"),
        @Index(name = "idx_outbox_aggregate", columnList = "aggregateType, aggregateId")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxMessage {

    @Id
    @Column(nullable = false, length = 64)
    private String id;

    @Column(nullable = false, length = 64)
    private String aggregateType; // e.g. "BET"

    @Column(nullable = false, length = 64)
    private String aggregateId;   // e.g. betId

    @Column(nullable = false, length = 64)
    private String eventType;     // e.g. "BET_SETTLED"

    @Column(nullable = false, length = 128)
    private String topic;         // e.g. "bet-settlements"

    @Lob
    @Column(nullable = false, columnDefinition = "CLOB")
    private String payload;       // JSON serialized payload

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(nullable = false)
    @Builder.Default
    private int retryCount = 0;

    @Column(length = 1024)
    private String errorMessage;

    @Column(length = 64)
    private String traceId;

    @Column(nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    private Instant processedAt;
}
