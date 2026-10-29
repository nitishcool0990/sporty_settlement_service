package com.sportygroup.settlement.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportygroup.settlement.config.AppProperties;
import com.sportygroup.settlement.dto.message.BetSettlementMessage;
import com.sportygroup.settlement.service.BetSettlementConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.rocketmq.enabled", havingValue = "true")
public class RocketMqBetSettlementConsumerService {

    private final AppProperties appProperties;
    private final BetSettlementConsumer settlementConsumer;
    private final ObjectMapper objectMapper;

    private DefaultMQPushConsumer pushConsumer;

    @PostConstruct
    public void start() {
        try {
            String nameServer = appProperties.getRocketmq().getNameServer();
            String consumerGroup = appProperties.getRocketmq().getConsumerGroup();
            String topic = appProperties.getRocketmq().getTopics().getBetSettlements();

            log.info("Starting RocketMQ Push Consumer: namesrv={}, group={}, topic={}", nameServer, consumerGroup, topic);

            pushConsumer = new DefaultMQPushConsumer(consumerGroup);
            pushConsumer.setNamesrvAddr(nameServer);
            pushConsumer.subscribe(topic, "*");

            pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        try {
                            BetSettlementMessage message = objectMapper.readValue(msg.getBody(), BetSettlementMessage.class);
                            settlementConsumer.handleSettlement(message);
                        } catch (Exception e) {
                            log.error("Failed to parse or process RocketMQ message id={}: {}", msg.getMsgId(), e.getMessage(), e);
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            pushConsumer.start();
            log.info("RocketMQ Push Consumer started successfully.");
        } catch (Exception ex) {
            log.error("Failed to start RocketMQ Push Consumer: {}", ex.getMessage(), ex);
            throw new IllegalStateException("Failed to start RocketMQ Push Consumer", ex);
        }
    }

    @PreDestroy
    public void stop() {
        if (pushConsumer != null) {
            log.info("Stopping RocketMQ Push Consumer...");
            pushConsumer.shutdown();
        }
    }
}
