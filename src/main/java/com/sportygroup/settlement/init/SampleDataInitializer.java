package com.sportygroup.settlement.init;

import com.sportygroup.settlement.domain.model.Bet;
import com.sportygroup.settlement.domain.model.BetOutcome;
import com.sportygroup.settlement.domain.model.BetStatus;
import com.sportygroup.settlement.domain.repository.BetRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SampleDataInitializer implements CommandLineRunner {

    private final BetRepository betRepository;

    @Override
    public void run(String... args) {
        log.info("Seeding in-memory H2 database with realistic sample sports bets...");

        List<Bet> initialBets = List.of(
                Bet.builder()
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
                        .build(),

                Bet.builder()
                        .betId("BET-1002")
                        .userId("USR-BOB")
                        .eventId("EVT-2026-CHEL-ARS")
                        .eventMarketId("MKT-MATCH-WINNER")
                        .eventWinnerId("TEAM-CHEL")
                        .betAmount(BigDecimal.valueOf(50.00))
                        .odds(BigDecimal.valueOf(1.85))
                        .status(BetStatus.PENDING)
                        .outcome(BetOutcome.PENDING)
                        .createdAt(Instant.now())
                        .build(),

                Bet.builder()
                        .betId("BET-1003")
                        .userId("USR-CHARLIE")
                        .eventId("EVT-2026-CHEL-ARS")
                        .eventMarketId("MKT-MATCH-WINNER")
                        .eventWinnerId("TEAM-ARS")
                        .betAmount(BigDecimal.valueOf(200.00))
                        .odds(BigDecimal.valueOf(2.20))
                        .status(BetStatus.PENDING)
                        .outcome(BetOutcome.PENDING)
                        .createdAt(Instant.now())
                        .build(),

                Bet.builder()
                        .betId("BET-2001")
                        .userId("USR-DAVE")
                        .eventId("EVT-2026-LIV-MCI")
                        .eventMarketId("MKT-MATCH-WINNER")
                        .eventWinnerId("TEAM-LIV")
                        .betAmount(BigDecimal.valueOf(75.00))
                        .odds(BigDecimal.valueOf(2.50))
                        .status(BetStatus.PENDING)
                        .outcome(BetOutcome.PENDING)
                        .createdAt(Instant.now())
                        .build(),

                Bet.builder()
                        .betId("BET-2002")
                        .userId("USR-EMMA")
                        .eventId("EVT-2026-LIV-MCI")
                        .eventMarketId("MKT-MATCH-WINNER")
                        .eventWinnerId("TEAM-MCI")
                        .betAmount(BigDecimal.valueOf(150.00))
                        .odds(BigDecimal.valueOf(1.65))
                        .status(BetStatus.PENDING)
                        .outcome(BetOutcome.PENDING)
                        .createdAt(Instant.now())
                        .build()
        );

        betRepository.saveAll(initialBets);
        log.info("Successfully seeded {} initial bets into in-memory database.", initialBets.size());
    }
}
