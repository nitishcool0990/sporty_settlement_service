package com.sportygroup.settlement.dto.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventOutcomeMessage {
    private String eventId;
    private String eventName;
    private String eventWinnerId;
    private String traceId;
    @Builder.Default
    private Instant publishedAt = Instant.now();
}
