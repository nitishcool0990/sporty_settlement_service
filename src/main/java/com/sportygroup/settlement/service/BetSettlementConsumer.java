package com.sportygroup.settlement.service;

import com.sportygroup.settlement.dto.message.BetSettlementMessage;

public interface BetSettlementConsumer {

    void handleSettlement(BetSettlementMessage message);
}
