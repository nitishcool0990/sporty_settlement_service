package com.sportygroup.settlement.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Map;
import java.util.concurrent.Executor;

@Slf4j
@EnableAsync
@Configuration
@RequiredArgsConstructor
public class MatchingExecutorConfig {

    private final AppProperties appProperties;

    /**
     * Dedicated thread pool for executing bet matching and settlement tasks concurrently in parallel.
     * Propagates MDC logging context (traceId, eventId) to each background worker thread.
     */
    @Bean(name = "betMatchingExecutor")
    public Executor betMatchingExecutor() {
        AppProperties.Matching.ThreadPool poolConfig = appProperties.getMatching().getThreadPool();

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolConfig.getCoreSize());
        executor.setMaxPoolSize(poolConfig.getMaxSize());
        executor.setQueueCapacity(poolConfig.getQueueCapacity());
        executor.setThreadNamePrefix(poolConfig.getThreadNamePrefix());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(20);

        // Decorate tasks with MDC logging context so background threads retain traceId and eventId
        executor.setTaskDecorator(new MdcTaskDecorator());
        executor.initialize();

        log.info("Initialized Bet Matching Thread Pool: core={}, max={}, queue={}, prefix={}",
                poolConfig.getCoreSize(), poolConfig.getMaxSize(), poolConfig.getQueueCapacity(), poolConfig.getThreadNamePrefix());

        return executor;
    }

    public static class MdcTaskDecorator implements TaskDecorator {
        @Override
        public Runnable decorate(Runnable runnable) {
            Map<String, String> contextMap = MDC.getCopyOfContextMap();
            return () -> {
                try {
                    if (contextMap != null) {
                        MDC.setContextMap(contextMap);
                    }
                    runnable.run();
                } finally {
                    MDC.clear();
                }
            };
        }
    }
}
