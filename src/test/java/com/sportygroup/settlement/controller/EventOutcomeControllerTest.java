package com.sportygroup.settlement.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.dto.message.EventOutcomeMessage;
import com.sportygroup.settlement.dto.request.EventOutcomeRequest;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.service.EventOutcomePublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = EventOutcomeController.class, properties = "app.kafka.embedded.enabled=false")
class EventOutcomeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private EventOutcomePublisher eventOutcomePublisher;

    @MockBean
    private CentralizedAuditService auditService;

    @MockBean
    private AppProperties appProperties;

    @Test
    @DisplayName("Should accept valid event outcome and dispatch to Kafka asynchronously")
    void shouldAcceptValidOutcome() throws Exception {
        EventOutcomeRequest request = EventOutcomeRequest.builder()
                .eventId("EVT-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId("TEAM-ARS")
                .build();

        AppProperties.Kafka kafka = new AppProperties.Kafka();
        kafka.getTopics().setEventOutcomes("event-outcomes");
        when(appProperties.getKafka()).thenReturn(kafka);

        AppProperties.Security security = new AppProperties.Security();
        when(appProperties.getSecurity()).thenReturn(security);

        EventOutcomeMessage message = EventOutcomeMessage.builder()
                .eventId("EVT-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId("TEAM-ARS")
                .traceId("test-trace")
                .publishedAt(Instant.now())
                .build();

        when(eventOutcomePublisher.publishOutcome(any(EventOutcomeRequest.class), any()))
                .thenReturn(CompletableFuture.completedFuture(message));

        var mvcResult = mockMvc.perform(post("/api/v1/events/outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.eventId").value("EVT-CHEL-ARS"))
                .andExpect(jsonPath("$.data.eventWinnerId").value("TEAM-ARS"));
    }

    @Test
    @DisplayName("Should reject request with missing fields with 400 Bad Request")
    void shouldRejectInvalidOutcome() throws Exception {
        EventOutcomeRequest invalidRequest = EventOutcomeRequest.builder()
                .eventId("") // blank
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId(null) // null
                .build();

        AppProperties.Security security = new AppProperties.Security();
        when(appProperties.getSecurity()).thenReturn(security);

        mockMvc.perform(post("/api/v1/events/outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidRequest)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.data.eventId").exists())
                .andExpect(jsonPath("$.data.eventWinnerId").exists());
    }

    @Test
    @DisplayName("Should reject request from unauthenticated domain with 403 Forbidden when domain check is enabled")
    void shouldRejectUnauthenticatedDomain() throws Exception {
        EventOutcomeRequest request = EventOutcomeRequest.builder()
                .eventId("EVT-CHEL-ARS")
                .eventName("Chelsea vs Arsenal")
                .eventWinnerId("TEAM-ARS")
                .build();

        AppProperties.Security security = new AppProperties.Security();
        security.setDomainCheckEnabled(true);
        security.setAllowedDomains(java.util.List.of("sportygroup.com", "*.sportybet.com"));
        when(appProperties.getSecurity()).thenReturn(security);

        // Request coming from unauthorized domain attacker.com
        mockMvc.perform(post("/api/v1/events/outcomes")
                        .header("Origin", "https://attacker.com")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("not authenticated")));
    }
}
