package com.sportygroup.settlement.service;

import com.sportygroup.settlement.dto.message.BetSettlementMessage;

import java.util.concurrent.CompletableFuture;

public interface BetSettlementProducer {

    CompletableFuture<Void> sendSettlement(BetSettlementMessage settlementMessage);
}
