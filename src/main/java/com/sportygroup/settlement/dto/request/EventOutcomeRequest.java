package com.sportygroup.settlement.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Sports event outcome payload to publish to Kafka")
public class EventOutcomeRequest {

    @NotBlank(message = "Event ID is required")
    @Schema(description = "Unique sports event identifier", example = "EVT-2026-CHEL-ARS")
    private String eventId;

    @NotBlank(message = "Event name is required")
    @Schema(description = "Descriptive name of the event", example = "Chelsea vs Arsenal")
    private String eventName;

    @NotBlank(message = "Event winner ID is required")
    @Schema(description = "Identifier of the winning participant / outcome", example = "TEAM-ARS")
    private String eventWinnerId;
}
