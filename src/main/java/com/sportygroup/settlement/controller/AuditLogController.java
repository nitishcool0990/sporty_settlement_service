package com.sportygroup.settlement.controller;

import com.sportygroup.settlement.domain.model.AuditLogEntry;
import com.sportygroup.settlement.domain.repository.AuditLogRepository;
import com.sportygroup.settlement.dto.response.ApiResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@RestController
@RequestMapping("/api/v1/audit")
@RequiredArgsConstructor
@Tag(name = "Centralized Observability & Audit Logs", description = "Query centralized log trace across the entire settlement pipeline")
public class AuditLogController {

    private final AuditLogRepository auditLogRepository;

    @GetMapping("/logs")
    @Operation(summary = "Query centralized logs", description = "Query audit logs across REST -> Kafka -> Matching -> RocketMQ -> DB")
    public ResponseEntity<ApiResponse<List<AuditLogEntry>>> getLogs(
            @RequestParam(required = false) String eventId,
            @RequestParam(required = false) String betId,
            @RequestParam(required = false) String traceId,
            @RequestParam(required = false) String stage) {

        List<AuditLogEntry> logs = auditLogRepository.findByFilter(eventId, betId, traceId, stage);
        String currentTrace = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Retrieved " + logs.size() + " centralized log entries", logs, currentTrace));
    }

    @DeleteMapping("/logs")
    @Operation(summary = "Clear audit logs", description = "Clears the in-memory centralized audit log store")
    public ResponseEntity<ApiResponse<Void>> clearLogs() {
        auditLogRepository.clear();
        String currentTrace = MDC.get("traceId");
        return ResponseEntity.ok(ApiResponse.ok("Centralized audit logs cleared successfully", null, currentTrace));
    }

    @GetMapping(value = "/logs/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Live stream audit logs (SSE)", description = "Streams newly recorded audit log entries in real-time via Server-Sent Events in chronological order")
    public SseEmitter streamLogs() {
        SseEmitter emitter = new SseEmitter(180_000L); // 3-minute timeout
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        // Sequence number tracking: avoids size-cap stalls and supports clearing logs
        final AtomicLong lastSeenSequenceNumber = new AtomicLong(auditLogRepository.getLatestSequenceNumber());

        executor.scheduleAtFixedRate(() -> {
            try {
                long latestSeq = auditLogRepository.getLatestSequenceNumber();
                // If logs were cleared, reset tracker
                if (latestSeq < lastSeenSequenceNumber.get()) {
                    lastSeenSequenceNumber.set(0);
                }

                List<AuditLogEntry> newLogs = auditLogRepository.findSinceSequence(lastSeenSequenceNumber.get());
                for (AuditLogEntry entry : newLogs) {
                    emitter.send(SseEmitter.event()
                            .name("log-event")
                            .id(String.valueOf(entry.getSequenceNumber()))
                            .data(entry));
                    lastSeenSequenceNumber.set(entry.getSequenceNumber());
                }
            } catch (IOException e) {
                emitter.complete();
                executor.shutdown();
            } catch (Exception ex) {
                log.debug("Error in log stream: {}", ex.getMessage());
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        emitter.onCompletion(executor::shutdown);
        emitter.onTimeout(executor::shutdown);
        emitter.onError(t -> executor.shutdown());

        return emitter;
    }
}
