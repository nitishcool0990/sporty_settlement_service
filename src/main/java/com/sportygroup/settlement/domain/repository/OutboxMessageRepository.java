package com.sportygroup.settlement.domain.repository;

import com.sportygroup.settlement.domain.model.OutboxMessage;
import com.sportygroup.settlement.domain.model.OutboxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, String> {

    @Query("SELECT o FROM OutboxMessage o WHERE o.status = :status ORDER BY o.createdAt ASC")
    List<OutboxMessage> findByStatusOrderByCreatedAtAsc(@Param("status") OutboxStatus status, Pageable pageable);

    List<OutboxMessage> findByStatus(OutboxStatus status);

    List<OutboxMessage> findByAggregateId(String aggregateId);

    long countByStatus(OutboxStatus status);
}
