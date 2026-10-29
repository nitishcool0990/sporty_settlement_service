package com.sportygroup.settlement.service;

import java.util.Map;

public interface CentralizedAuditService {

    void record(String traceId, String stage, String eventId, String betId, String userId, String message, Map<String, Object> payload);

    void record(String stage, String eventId, String message);
}
