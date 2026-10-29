package com.sportygroup.settlement.controller;

import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.dto.request.EventOutcomeRequest;
import com.sportygroup.settlement.dto.response.ApiResponse;
import com.sportygroup.settlement.dto.response.SettlementSummaryResponse;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.service.EventOutcomePublisher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Tag(name = "Event Outcomes", description = "Endpoints for publishing sports event outcomes to Kafka")
public class EventOutcomeController {

    private final EventOutcomePublisher eventOutcomePublisher;
    private final CentralizedAuditService auditService;
    private final AppProperties appProperties;

    @PostMapping("/outcomes")
    @Operation(summary = "Publish sports event outcome to Kafka",
               description = "Dispatches the outcome of a match to trigger settlement matching downstream.")
    public CompletableFuture<ResponseEntity<ApiResponse<SettlementSummaryResponse>>> publishOutcome(
            @Valid @RequestBody EventOutcomeRequest request) {

        String traceId = MDC.get("traceId");
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString();
            MDC.put("traceId", traceId);
        }

        MDC.put("eventId", request.getEventId());

        log.info("[REST-API] Received event outcome publication request: eventId={}, winnerId={}",
                request.getEventId(), request.getEventWinnerId());

        auditService.record(
                traceId,
                "EVENT_OUTCOME_RECEIVED",
                request.getEventId(),
                null,
                null,
                "Received HTTP POST /api/v1/events/outcomes for event: " + request.getEventId(),
                com.sportygroup.settlement.util.SafeMap.of(
                        "eventName", request.getEventName(),
                        "winnerId", request.getEventWinnerId()
                )
        );

        String finalTraceId = traceId;
        String topic = appProperties.getKafka().getTopics().getEventOutcomes();

        return eventOutcomePublisher.publishOutcome(request, finalTraceId)
                .thenApply(published -> {
                    SettlementSummaryResponse summary = SettlementSummaryResponse.builder()
                            .eventId(request.getEventId())
                            .eventName(request.getEventName())
                            .eventWinnerId(request.getEventWinnerId())
                            .kafkaTopic(topic)
                            .traceId(finalTraceId)
                            .status("PUBLISHED_TO_KAFKA")
                            .timestamp(Instant.now())
                            .build();

                    return ResponseEntity.status(HttpStatus.ACCEPTED)
                            .body(ApiResponse.ok("Sports event outcome published successfully to Kafka", summary, finalTraceId));
                });
    }
}
