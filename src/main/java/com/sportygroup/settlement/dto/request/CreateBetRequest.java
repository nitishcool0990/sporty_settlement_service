package com.sportygroup.settlement.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Payload to register a new bet")
public class CreateBetRequest {

    @Schema(description = "Unique bet identifier (auto-generated if omitted)", example = "BET-999")
    private String betId;

    @NotBlank(message = "User ID is required")
    @Schema(description = "User who placed the bet", example = "USR-101")
    private String userId;

    @NotBlank(message = "Event ID is required")
    @Schema(description = "Target sports event ID", example = "EVT-2026-CHEL-ARS")
    private String eventId;

    @NotBlank(message = "Market ID is required")
    @Schema(description = "Betting market ID (e.g. Match Winner, Total Goals)", example = "MKT-MATCH-WINNER")
    private String eventMarketId;

    @NotBlank(message = "Predicted winner ID is required")
    @Schema(description = "The participant/outcome the user bet on", example = "TEAM-ARS")
    private String eventWinnerId;

    @NotNull(message = "Bet amount is required")
    @DecimalMin(value = "0.01", message = "Bet amount must be greater than zero")
    @Schema(description = "Amount wagered", example = "50.00")
    private BigDecimal betAmount;

    @DecimalMin(value = "1.01", message = "Odds must be at least 1.01")
    @Schema(description = "Decimal odds multiplier", example = "2.10")
    private BigDecimal odds;
}
