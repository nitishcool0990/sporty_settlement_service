package com.sportygroup.settlement.service;

import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.dto.request.EventOutcomeRequest;

import java.util.concurrent.CompletableFuture;

public interface EventOutcomePublisher {

    CompletableFuture<EventOutcomeMessage> publishOutcome(EventOutcomeRequest request, String traceId);
}
