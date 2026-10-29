package com.sportygroup.settlement.dto.response;

import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
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
public class BetResponse {
    private String betId;
    private String userId;
    private String eventId;
    private String eventMarketId;
    private String eventWinnerId;
    private BigDecimal betAmount;
    private BigDecimal odds;
    private BigDecimal payoutAmount;
    private BetStatus status;
    private BetOutcome outcome;
    private Instant createdAt;
    private Instant settledAt;
    private String settlementTraceId;

    public static BetResponse from(Bet bet) {
        return BetResponse.builder()
                .betId(bet.getBetId())
                .userId(bet.getUserId())
                .eventId(bet.getEventId())
                .eventMarketId(bet.getEventMarketId())
                .eventWinnerId(bet.getEventWinnerId())
                .betAmount(bet.getBetAmount())
                .odds(bet.getOdds())
                .payoutAmount(bet.getPayoutAmount())
                .status(bet.getStatus())
                .outcome(bet.getOutcome())
                .createdAt(bet.getCreatedAt())
                .settledAt(bet.getSettledAt())
                .settlementTraceId(bet.getSettlementTraceId())
                .build();
    }
}
