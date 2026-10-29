package com.sportygroup.settlement.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private Security security = new Security();
    private Kafka kafka = new Kafka();
    private RocketMq rocketmq = new RocketMq();
    private Logging logging = new Logging();
    private Matching matching = new Matching();
    private Outbox outbox = new Outbox();

    @Getter
    @Setter
    public static class Security {
        private String authToken = "";
        private String authHeaderName = "X-Auth-Token";
        private boolean domainCheckEnabled = false;
        private List<String> allowedDomains = new ArrayList<>();

        public boolean isAuthEnabled() {
            return authToken != null && !authToken.trim().isEmpty();
        }

        public boolean isDomainCheckActive() {
            return domainCheckEnabled || (allowedDomains != null && !allowedDomains.isEmpty());
        }
    }

    @Getter
    @Setter
    public static class Kafka {
        private int concurrency = 3;
        private Topics topics = new Topics();

        @Getter
        @Setter
        public static class Topics {
            private String eventOutcomes = "event-outcomes";
        }
    }

    @Getter
    @Setter
    public static class Matching {
        private ThreadPool threadPool = new ThreadPool();

        @Getter
        @Setter
        public static class ThreadPool {
            private int coreSize = 10;
            private int maxSize = 50;
            private int queueCapacity = 500;
            private String threadNamePrefix = "bet-matcher-";
        }
    }

    @Getter
    @Setter
    public static class RocketMq {
        private boolean enabled = false;
        private String nameServer = "localhost:9876";
        private String producerGroup = "sporty-bet-settlement-producer-group";
        private String consumerGroup = "sporty-bet-settlement-consumer-group";
        private Topics topics = new Topics();

        @Getter
        @Setter
        public static class Topics {
            private String betSettlements = "bet-settlements";
        }
    }

    @Getter
    @Setter
    public static class Logging {
        private Loki loki = new Loki();
        private Audit audit = new Audit();

        @Getter
        @Setter
        public static class Loki {
            private boolean enabled = false;
            private String url = "http://localhost:3100/loki/api/v1/push";
        }

        @Getter
        @Setter
        public static class Audit {
            private int maxInMemoryEntries = 1000;
        }
    }

    @Getter
    @Setter
    public static class Outbox {
        private boolean enabled = true;
        private int pollIntervalMs = 500;
        private int batchSize = 50;
        private int maxRetries = 5;
    }
}
