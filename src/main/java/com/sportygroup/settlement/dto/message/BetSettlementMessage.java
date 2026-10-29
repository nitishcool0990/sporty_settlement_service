package com.sportygroup.settlement.dto.message;

import com.sportygroup.settlement.domain.model.BetOutcome;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BetSettlementMessage {
    private String betId;
    private String userId;
    private String eventId;
    private String eventMarketId;
    private String eventWinnerId;
    private String actualWinnerId;
    private BigDecimal betAmount;
    private BigDecimal odds;
    private BigDecimal payoutAmount;
    private BetOutcome outcome; // WON or LOST
    private String traceId;
    @Builder.Default
    private Instant dispatchedAt = Instant.now();
}
