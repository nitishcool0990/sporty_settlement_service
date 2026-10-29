package com.sportygroup.settlement.domain.repository;

import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.AuditLogEntry;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
public class AuditLogRepository {

    private final Deque<AuditLogEntry> logs = new ConcurrentLinkedDeque<>();
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final int maxCapacity;

    public AuditLogRepository(AppProperties appProperties) {
        this.maxCapacity = appProperties.getLogging().getAudit().getMaxInMemoryEntries();
    }

    public synchronized void save(AuditLogEntry entry) {
        entry.setSequenceNumber(sequenceCounter.incrementAndGet());
        logs.addFirst(entry);
        while (logs.size() > maxCapacity) {
            logs.removeLast();
        }
    }

    public List<AuditLogEntry> findAll() {
        return new ArrayList<>(logs);
    }

    public List<AuditLogEntry> findByFilter(String eventId, String betId, String traceId, String stage) {
        return logs.stream()
                .filter(entry -> eventId == null || eventId.equalsIgnoreCase(entry.getEventId()))
                .filter(entry -> betId == null || betId.equalsIgnoreCase(entry.getBetId()))
                .filter(entry -> traceId == null || traceId.equalsIgnoreCase(entry.getTraceId()))
                .filter(entry -> stage == null || stage.equalsIgnoreCase(entry.getStage()))
                .collect(Collectors.toList());
    }

    /**
     * Returns newly added logs after a given sequence number, ordered chronologically (oldest to newest).
     */
    public List<AuditLogEntry> findSinceSequence(long sinceSequence) {
        List<AuditLogEntry> recent = new ArrayList<>();
        for (AuditLogEntry entry : logs) {
            if (entry.getSequenceNumber() > sinceSequence) {
                recent.add(entry);
            }
        }
        // Logs are stored newest-first in the deque, so reverse to stream chronologically
        Collections.reverse(recent);
        return recent;
    }

    public long getLatestSequenceNumber() {
        return sequenceCounter.get();
    }

    public synchronized void clear() {
        logs.clear();
        sequenceCounter.set(0);
    }
}
