package com.sportygroup.settlement.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.service.BetSettlementProducer;
import com.sportygroup.settlement.service.CentralizedAuditService;
import com.sportygroup.settlement.util.SafeMap;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.rocketmq.enabled", havingValue = "true")
public class RocketMqBetSettlementProducer implements BetSettlementProducer {

    private final AppProperties appProperties;
    private final CentralizedAuditService auditService;
    private final ObjectMapper objectMapper;

    private DefaultMQProducer producer;

    @PostConstruct
    public void init() {
        try {
            String nameServer = appProperties.getRocketmq().getNameServer();
            String producerGroup = appProperties.getRocketmq().getProducerGroup();
            log.info("Initializing RocketMQ Producer: namesrv={}, group={}", nameServer, producerGroup);

            producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(nameServer);
            producer.setRetryTimesWhenSendAsyncFailed(2);
            producer.start();
            log.info("RocketMQ Producer started successfully.");
        } catch (Exception ex) {
            log.error("Failed to start RocketMQ Producer: {}", ex.getMessage(), ex);
            throw new IllegalStateException("Failed to start RocketMQ Producer", ex);
        }
    }

    @PreDestroy
    public void destroy() {
        if (producer != null) {
            log.info("Shutting down RocketMQ Producer...");
            producer.shutdown();
        }
    }

    @Override
    public CompletableFuture<Void> sendSettlement(BetSettlementMessage settlementMessage) {
        String topic = appProperties.getRocketmq().getTopics().getBetSettlements();
        CompletableFuture<Void> future = new CompletableFuture<>();

        try {
            byte[] body = objectMapper.writeValueAsBytes(settlementMessage);
            Message msg = new Message(topic, "BET_SETTLEMENT", settlementMessage.getBetId(), body);

            if (settlementMessage.getTraceId() != null) {
                msg.putUserProperty("traceId", settlementMessage.getTraceId());
            }
            if (settlementMessage.getEventId() != null) {
                msg.putUserProperty("eventId", settlementMessage.getEventId());
            }
            msg.putUserProperty("betId", settlementMessage.getBetId());

            log.info("Sending settlement message to RocketMQ topic [{}]: betId={}", topic, settlementMessage.getBetId());

            producer.send(msg, new org.apache.rocketmq.client.producer.SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    try {
                        log.info("RocketMQ send success: msgId={}, status={}",
                                sendResult.getMsgId(), sendResult.getSendStatus());

                        auditService.record(
                                settlementMessage.getTraceId(),
                                "ROCKETMQ_PUBLISHED",
                                settlementMessage.getEventId(),
                                settlementMessage.getBetId(),
                                settlementMessage.getUserId(),
                                "Dispatched bet settlement to RocketMQ topic: " + topic,
                                SafeMap.of(
                                        "msgId", sendResult.getMsgId(),
                                        "sendStatus", sendResult.getSendStatus() != null ? sendResult.getSendStatus().name() : "OK",
                                        "betId", settlementMessage.getBetId()
                                )
                        );
                    } finally {
                        future.complete(null);
                    }
                }

                @Override
                public void onException(Throwable throwable) {
                    String errorMsg = throwable != null && throwable.getMessage() != null
                            ? throwable.getMessage()
                            : (throwable != null ? throwable.getClass().getSimpleName() : "Unknown RocketMQ send error");

                    try {
                        log.error("Failed to send settlement to RocketMQ topic [{}]: {}", topic, errorMsg, throwable);
                        auditService.record(
                                settlementMessage.getTraceId(),
                                "ROCKETMQ_PUBLISH_FAILED",
                                settlementMessage.getEventId(),
                                settlementMessage.getBetId(),
                                settlementMessage.getUserId(),
                                "Failed to dispatch to RocketMQ: " + errorMsg,
                                SafeMap.of("error", errorMsg)
                        );
                    } finally {
                        future.completeExceptionally(throwable != null ? throwable : new RuntimeException(errorMsg));
                    }
                }
            });
        } catch (Exception e) {
            log.error("Error creating RocketMQ message: {}", e.getMessage(), e);
            future.completeExceptionally(e);
        }

        return future;
    }
}
