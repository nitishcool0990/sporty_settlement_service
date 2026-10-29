package com.sportygroup.settlement.controller;

import com.sportygroup.settlement.domain.model.OutboxMessage;
import com.sportygroup.settlement.domain.model.OutboxStatus;
import com.sportygroup.settlement.domain.repository.OutboxMessageRepository;
import com.sportygroup.settlement.dto.response.ApiResponse;
import com.sportygroup.settlement.service.OutboxService;
import com.sportygroup.settlement.util.SafeMap;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/outbox")
@RequiredArgsConstructor
@Tag(name = "Transactional Outbox Management", description = "Monitor, inspect, and replay transactional outbox messages")
public class OutboxController {

    private final OutboxService outboxService;
    private final OutboxMessageRepository outboxRepository;

    @GetMapping("/messages")
    @Operation(summary = "Get outbox messages", description = "Query outbox messages optionally filtered by status")
    public ResponseEntity<ApiResponse<List<OutboxMessage>>> getMessages(
            @RequestParam(required = false) OutboxStatus status) {
        List<OutboxMessage> messages = (status != null)
                ? outboxRepository.findByStatus(status)
                : outboxRepository.findAll();

        String traceId = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Retrieved " + messages.size() + " outbox messages", messages, traceId));
    }

    @GetMapping("/stats")
    @Operation(summary = "Get outbox statistics", description = "Get aggregate count of PENDING, PUBLISHED, and FAILED outbox messages")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getStats() {
        long pending = outboxRepository.countByStatus(OutboxStatus.PENDING);
        long published = outboxRepository.countByStatus(OutboxStatus.PUBLISHED);
        long failed = outboxRepository.countByStatus(OutboxStatus.FAILED);
        long total = outboxRepository.count();

        Map<String, Object> stats = SafeMap.of(
                "total", total,
                "pending", pending,
                "published", published,
                "failed", failed
        );

        String traceId = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Outbox statistics", stats, traceId));
    }

    @PostMapping("/relay")
    @Operation(summary = "Trigger immediate relay", description = "Manually triggers an immediate outbox dispatch cycle")
    public ResponseEntity<ApiResponse<Map<String, Object>>> triggerRelay() {
        int dispatched = outboxService.processPendingMessages();
        String traceId = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Outbox relay processed", SafeMap.of("dispatchedCount", dispatched), traceId));
    }

    @PostMapping("/retry-failed")
    @Operation(summary = "Retry dead-letter failed messages", description = "Re-arms all FAILED outbox messages back to PENDING and triggers immediate relay")
    public ResponseEntity<ApiResponse<Map<String, Object>>> retryFailed() {
        int resetCount = outboxService.retryFailedMessages();
        String traceId = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Re-armed failed messages for replay", SafeMap.of("rearmedCount", resetCount), traceId));
    }
}
