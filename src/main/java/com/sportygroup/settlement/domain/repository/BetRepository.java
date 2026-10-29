package com.sportygroup.settlement.domain.repository;

import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Repository
public interface BetRepository extends JpaRepository<Bet, String> {

    List<Bet> findByEventId(String eventId);

    List<Bet> findByEventIdAndStatus(String eventId, BetStatus status);

    List<Bet> findByStatus(BetStatus status);

    List<Bet> findByUserId(String userId);

    @Query("SELECT b FROM Bet b WHERE " +
           "(:eventId IS NULL OR b.eventId = :eventId) AND " +
           "(:status IS NULL OR b.status = :status) AND " +
           "(:userId IS NULL OR b.userId = :userId)")
    List<Bet> findWithFilters(
            @Param("eventId") String eventId,
            @Param("status") BetStatus status,
            @Param("userId") String userId
    );

    @Modifying
    @Query("UPDATE Bet b SET b.status = :newStatus, b.outcome = :outcome, b.payoutAmount = :payoutAmount, " +
           "b.settledAt = :settledAt, b.settlementTraceId = :traceId, b.version = COALESCE(b.version, 0L) + 1L " +
           "WHERE b.betId = :betId AND b.status = :expectedStatus")
    int updateBetSettlementIfStatus(
            @Param("betId") String betId,
            @Param("expectedStatus") BetStatus expectedStatus,
            @Param("newStatus") BetStatus newStatus,
            @Param("outcome") BetOutcome outcome,
            @Param("payoutAmount") BigDecimal payoutAmount,
            @Param("settledAt") Instant settledAt,
            @Param("traceId") String traceId
    );
}
