package com.sportygroup.settlement.service;

import com.sportygroup.settlement.dto.message.EventOutcomeMessage;

import java.util.concurrent.CompletableFuture;

public interface BetMatchingService {

    void matchAndTriggerSettlements(EventOutcomeMessage outcomeMessage);

    CompletableFuture<Void> matchAndTriggerSettlementsAsync(EventOutcomeMessage outcomeMessage);
}
