package com.sportygroup.settlement.controller;

import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import com.sportygroup.settlement.dto.request.CreateBetRequest;
import com.sportygroup.settlement.dto.response.ApiResponse;
import com.sportygroup.settlement.dto.response.BetResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/bets")
@RequiredArgsConstructor
@Tag(name = "Bets", description = "Query and place bets in the in-memory database")
public class BetController {

    private final BetRepository betRepository;

    @GetMapping
    @Operation(summary = "List bets", description = "Retrieve all bets, supporting multi-criteria filtering by eventId, status, and userId simultaneously")
    public ResponseEntity<ApiResponse<List<BetResponse>>> getBets(
            @RequestParam(required = false) String eventId,
            @RequestParam(required = false) BetStatus status,
            @RequestParam(required = false) String userId) {

        // Multi-criteria combined query: never drops any provided filter
        List<Bet> bets = betRepository.findWithFilters(eventId, status, userId);

        List<BetResponse> responses = bets.stream()
                .map(BetResponse::from)
                .collect(Collectors.toList());

        String traceId = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Retrieved " + responses.size() + " bets", responses, traceId));
    }

    @GetMapping("/{betId}")
    @Operation(summary = "Get bet by ID", description = "Retrieve a specific bet by its unique identifier")
    public ResponseEntity<ApiResponse<BetResponse>> getBetById(@PathVariable String betId) {
        String traceId = MDC.get("traceId");
        return betRepository.findById(betId)
                .map(bet -> ResponseEntity.ok(ApiResponse.ok("Bet found", BetResponse.from(bet), traceId)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(ApiResponse.error("Bet not found with ID: " + betId, traceId)));
    }

    @PostMapping
    @Operation(summary = "Place a new bet", description = "Creates a new pending bet in the in-memory database. Rejects duplicate IDs with 409 Conflict.")
    public ResponseEntity<ApiResponse<BetResponse>> createBet(@Valid @RequestBody CreateBetRequest request) {
        String traceId = MDC.get("traceId");

        // Validate if a custom bet ID is provided and already exists in database
        if (request.getBetId() != null && !request.getBetId().isBlank()) {
            if (betRepository.existsById(request.getBetId())) {
                log.warn("Attempted to overwrite existing betId={}", request.getBetId());
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(ApiResponse.error("Bet with ID '" + request.getBetId() + "' already exists. Modification of existing bets is forbidden.", traceId));
            }
        }

        String betId = (request.getBetId() != null && !request.getBetId().isBlank())
                ? request.getBetId()
                : "BET-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Bet bet = Bet.builder()
                .betId(betId)
                .userId(request.getUserId())
                .eventId(request.getEventId())
                .eventMarketId(request.getEventMarketId())
                .eventWinnerId(request.getEventWinnerId())
                .betAmount(request.getBetAmount())
                .odds(request.getOdds() != null ? request.getOdds() : BigDecimal.valueOf(2.00))
                .status(BetStatus.PENDING)
                .outcome(BetOutcome.PENDING)
                .createdAt(Instant.now())
                .build();

        Bet saved = betRepository.save(bet);
        log.info("Registered new bet: id={}, user={}, event={}, winnerChoice={}, amount={}",
                saved.getBetId(), saved.getUserId(), saved.getEventId(), saved.getEventWinnerId(), saved.getBetAmount());

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.ok("Bet placed successfully", BetResponse.from(saved), traceId));
    }
}
