package com.sportygroup.settlement.config;

import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.util.Map;

/**
 * Standalone mode: starts an in-process KRaft Kafka broker before the application context is built,
 * and points spring.kafka.bootstrap-servers at it. This lets the full
 * REST -> Kafka -> matching -> settlement flow run with zero external infrastructure.
 * <p>
 * Enabled by app.kafka.embedded.enabled (default true); disabled in the docker profile,
 * where a real Kafka broker is used. Registered in META-INF/spring.factories.
 */
public class EmbeddedKafkaEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static final String PROPERTY_SOURCE_NAME = "embeddedKafka";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        boolean enabled = environment.getProperty("app.kafka.embedded.enabled", Boolean.class, false);
        if (!enabled || environment.getPropertySources().contains(PROPERTY_SOURCE_NAME)) {
            return;
        }

        String topic = environment.getProperty("app.kafka.topics.event-outcomes", "event-outcomes");

        // Broker binds a random free port, so it never clashes with a locally running Kafka
        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 3, topic);
        broker.afterPropertiesSet();
        String brokers = broker.getBrokersAsString();

        environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE_NAME,
                Map.of("spring.kafka.bootstrap-servers", brokers)));
        System.out.println("[EMBEDDED-KAFKA] Standalone mode: in-process Kafka broker started at " + brokers);

        application.addListeners(new BrokerLifecycleListener(broker));
    }

    /**
     * Registers the broker as a disposable bean before any other bean exists, so it is destroyed last,
     * after the Kafka listener containers and producer have shut down.
     */
    private record BrokerLifecycleListener(EmbeddedKafkaKraftBroker broker) implements ApplicationListener<ApplicationPreparedEvent> {

        @Override
        public void onApplicationEvent(ApplicationPreparedEvent event) {
            event.getApplicationContext().getBeanFactory()
                    .registerSingleton("embeddedKafkaBroker", broker);
            ((DefaultSingletonBeanRegistry) event.getApplicationContext().getBeanFactory())
                    .registerDisposableBean("embeddedKafkaBroker", broker);
        }
    }
}
