package com.sportygroup.settlement.controller;

import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.OutboxMessage;
import com.sportygroup.settlement.domain.model.OutboxStatus;
import com.sportygroup.settlement.domain.repository.OutboxMessageRepository;
import com.sportygroup.settlement.service.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = OutboxController.class, properties = "app.kafka.embedded.enabled=false")
@AutoConfigureMockMvc(addFilters = false)
class OutboxControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OutboxService outboxService;

    @MockBean
    private OutboxMessageRepository outboxRepository;

    @MockBean
    private AppProperties appProperties;

    @BeforeEach
    void setUp() {
        AppProperties.Security security = new AppProperties.Security();
        when(appProperties.getSecurity()).thenReturn(security);
    }

    @Test
    @DisplayName("GET /api/v1/outbox/stats should return aggregate counts")
    void shouldReturnOutboxStats() throws Exception {
        when(outboxRepository.count()).thenReturn(10L);
        when(outboxRepository.countByStatus(OutboxStatus.PENDING)).thenReturn(2L);
        when(outboxRepository.countByStatus(OutboxStatus.PUBLISHED)).thenReturn(7L);
        when(outboxRepository.countByStatus(OutboxStatus.FAILED)).thenReturn(1L);

        mockMvc.perform(get("/api/v1/outbox/stats")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.total").value(10))
                .andExpect(jsonPath("$.data.pending").value(2))
                .andExpect(jsonPath("$.data.published").value(7))
                .andExpect(jsonPath("$.data.failed").value(1));
    }

    @Test
    @DisplayName("GET /api/v1/outbox/messages should return list of outbox messages")
    void shouldReturnOutboxMessages() throws Exception {
        OutboxMessage msg = OutboxMessage.builder()
                .id("outbox-101")
                .aggregateType("BET")
                .aggregateId("BET-1")
                .eventType("BET_SETTLED")
                .topic("bet-settlements")
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .createdAt(Instant.now())
                .build();

        when(outboxRepository.findByStatus(OutboxStatus.PENDING)).thenReturn(List.of(msg));

        mockMvc.perform(get("/api/v1/outbox/messages?status=PENDING")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data[0].id").value("outbox-101"))
                .andExpect(jsonPath("$.data[0].status").value("PENDING"));
    }

    @Test
    @DisplayName("POST /api/v1/outbox/relay should trigger immediate outbox processing")
    void shouldTriggerRelay() throws Exception {
        when(outboxService.processPendingMessages()).thenReturn(3);

        mockMvc.perform(post("/api/v1/outbox/relay")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.dispatchedCount").value(3));

        verify(outboxService).processPendingMessages();
    }

    @Test
    @DisplayName("POST /api/v1/outbox/retry-failed should reset failed messages for replay")
    void shouldRetryFailedMessages() throws Exception {
        when(outboxService.retryFailedMessages()).thenReturn(2);

        mockMvc.perform(post("/api/v1/outbox/retry-failed")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.rearmedCount").value(2));

        verify(outboxService).retryFailedMessages();
    }
}
