package com.sportygroup.settlement.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import com.sportygroup.settlement.dto.request.CreateBetRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = BetController.class, properties = "app.kafka.embedded.enabled=false")
class BetControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private BetRepository betRepository;

    @MockBean
    private AppProperties appProperties;

    @BeforeEach
    void setUp() {
        AppProperties.Security security = new AppProperties.Security();
        when(appProperties.getSecurity()).thenReturn(security);
    }

    @Test
    @DisplayName("Should return 409 Conflict when attempting to create a bet with existing betId")
    void shouldReturn409OnDuplicateBetId() throws Exception {
        CreateBetRequest duplicateRequest = CreateBetRequest.builder()
                .betId("BET-1001")
                .userId("USR-HACKER")
                .eventId("EVT-2026-CHEL-ARS")
                .eventMarketId("MKT-MATCH-WINNER")
                .eventWinnerId("TEAM-CHEL")
                .betAmount(BigDecimal.valueOf(500.00))
                .build();

        when(betRepository.existsById("BET-1001")).thenReturn(true);

        mockMvc.perform(post("/api/v1/bets")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(duplicateRequest)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("already exists")));
    }

    @Test
    @DisplayName("Should filter bets concurrently by eventId, status, and userId without dropping filters")
    void shouldFilterBetsWithMultipleCriteria() throws Exception {
        Bet bet = Bet.builder()
                .betId("BET-1001")
                .userId("USR-ALICE")
                .eventId("EVT-2026-CHEL-ARS")
                .eventMarketId("MKT-MATCH-WINNER")
                .eventWinnerId("TEAM-ARS")
                .betAmount(BigDecimal.valueOf(100.00))
                .odds(BigDecimal.valueOf(2.20))
                .status(BetStatus.PENDING)
                .outcome(BetOutcome.PENDING)
                .createdAt(Instant.now())
                .build();

        when(betRepository.findWithFilters("EVT-2026-CHEL-ARS", BetStatus.PENDING, "USR-ALICE"))
                .thenReturn(List.of(bet));

        mockMvc.perform(get("/api/v1/bets")
                        .param("eventId", "EVT-2026-CHEL-ARS")
                        .param("status", "PENDING")
                        .param("userId", "USR-ALICE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data", hasSize(1)))
                .andExpect(jsonPath("$.data[0].betId").value("BET-1001"))
                .andExpect(jsonPath("$.data[0].userId").value("USR-ALICE"));
    }
}
