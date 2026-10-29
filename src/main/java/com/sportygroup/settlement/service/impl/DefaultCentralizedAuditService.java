package com.sportygroup.settlement.service.impl;

import com.sportygroup.settlement.domain.model.AuditLogEntry;
import com.sportygroup.settlement.domain.repository.AuditLogRepository;
import com.sportygroup.settlement.service.CentralizedAuditService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DefaultCentralizedAuditService implements CentralizedAuditService {

    private final AuditLogRepository auditLogRepository;

    @Override
    public void record(String traceId, String stage, String eventId, String betId, String userId, String message, Map<String, Object> payload) {
        String effectiveTraceId = traceId;
        if (effectiveTraceId == null || effectiveTraceId.isBlank()) {
            effectiveTraceId = MDC.get("traceId");
        }
        if (effectiveTraceId == null || effectiveTraceId.isBlank()) {
            effectiveTraceId = UUID.randomUUID().toString();
        }

        AuditLogEntry entry = AuditLogEntry.builder()
                .id(UUID.randomUUID().toString())
                .timestamp(Instant.now())
                .traceId(effectiveTraceId)
                .stage(stage)
                .eventId(eventId)
                .betId(betId)
                .userId(userId)
                .message(message)
                .payload(payload != null ? payload : Map.of())
                .build();

        auditLogRepository.save(entry);

        // Safely set MDC for structured logging, then restore or clean up to avoid thread-pollution
        String prevTrace = MDC.get("traceId");
        String prevStage = MDC.get("stage");
        String prevEvent = MDC.get("eventId");
        String prevBet = MDC.get("betId");
        String prevUser = MDC.get("userId");

        try {
            MDC.put("traceId", effectiveTraceId);
            MDC.put("stage", stage != null ? stage : "");
            if (eventId != null) MDC.put("eventId", eventId);
            if (betId != null) MDC.put("betId", betId);
            if (userId != null) MDC.put("userId", userId);

            log.info("[AUDIT] stage={} | eventId={} | betId={} | userId={} | msg={}",
                    stage, eventId, betId, userId, message);
        } finally {
            restoreOrRemove("traceId", prevTrace);
            restoreOrRemove("stage", prevStage);
            restoreOrRemove("eventId", prevEvent);
            restoreOrRemove("betId", prevBet);
            restoreOrRemove("userId", prevUser);
        }
    }

    @Override
    public void record(String stage, String eventId, String message) {
        record(null, stage, eventId, null, null, message, Map.of());
    }

    private void restoreOrRemove(String key, String previousValue) {
        if (previousValue != null) {
            MDC.put(key, previousValue);
        } else {
            MDC.remove(key);
        }
    }
}
