package com.sportygroup.settlement.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLogEntry {
    private long sequenceNumber;
    private String id;
    private Instant timestamp;
    private String traceId;
    private String stage; // e.g. "EVENT_OUTCOME_RECEIVED", "KAFKA_PUBLISHED", "KAFKA_CONSUMED", "BETS_MATCHED", "ROCKETMQ_PUBLISHED", "ROCKETMQ_CONSUMED", "BET_SETTLED"
    private String eventId;
    private String betId;
    private String userId;
    private String message;
    private Map<String, Object> payload;
}
