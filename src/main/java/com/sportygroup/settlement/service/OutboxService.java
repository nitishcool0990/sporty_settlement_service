package com.sportygroup.settlement.service;

import com.sportygroup.settlement.domain.model.OutboxMessage;

import java.util.List;

public interface OutboxService {

    /**
     * Persists an outgoing domain event into the outbox table within the caller's transaction.
     */
    OutboxMessage save(String aggregateType, String aggregateId, String eventType, String topic, Object payload, String traceId);

    /**
     * Polls and dispatches a batch of pending outbox messages to the message broker.
     * Marks messages as PUBLISHED on success, or increments retryCount / marks FAILED on error.
     *
     * @return number of successfully dispatched messages in this cycle.
     */
    int processPendingMessages();

    /**
     * Asynchronously triggers an immediate outbox relay cycle to minimize delivery latency.
     */
    void relayImmediatelyAsync();

    /**
     * Retrieves all pending outbox messages.
     */
    List<OutboxMessage> getPendingMessages();

    /**
     * Returns count of currently pending outbox messages.
     */
    long countPendingMessages();

    /**
     * Re-arms all FAILED messages back to PENDING with retryCount reset for dead-letter replay.
     */
    int retryFailedMessages();
}
