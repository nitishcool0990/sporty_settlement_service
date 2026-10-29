package com.sportygroup.settlement.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SettlementSummaryResponse {
    private String eventId;
    private String eventName;
    private String eventWinnerId;
    private String kafkaTopic;
    private String traceId;
    private String status;
    private Instant timestamp;
}
