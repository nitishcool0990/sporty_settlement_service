package com.sportygroup.settlement.config;

import com.sportygroup.settlement.util.DomainMatcher;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class WebMvcConfig implements WebMvcConfigurer {

    public static final String TRACE_ID_ATTRIBUTE = "SP_TRACE_ID_ATTR";

    /**
     * Filter 1: Injects and manages MDC traceId across both sync and async servlet dispatches,
     * ensuring trace continuity and guaranteed cleanup in finally.
     */
    @Configuration
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public static class TraceContextFilter extends OncePerRequestFilter {

        @Override
        protected boolean shouldNotFilterAsyncDispatch() {
            // Must execute on async dispatches as well so error-handling threads retain the traceId
            return false;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
                throws ServletException, IOException {

            String traceId = (String) request.getAttribute(TRACE_ID_ATTRIBUTE);
            if (traceId == null || traceId.isBlank()) {
                traceId = request.getHeader("X-Correlation-Id");
                if (traceId == null || traceId.isBlank()) {
                    traceId = request.getHeader("X-Trace-Id");
                }
                if (traceId == null || traceId.isBlank()) {
                    traceId = UUID.randomUUID().toString();
                }
                request.setAttribute(TRACE_ID_ATTRIBUTE, traceId);
            }

            try {
                MDC.put("traceId", traceId);
                response.setHeader("X-Correlation-Id", traceId);
                filterChain.doFilter(request, response);
            } finally {
                MDC.clear();
            }
        }
    }

    /**
     * Filter 2: Security authentication filter.
     * Validates:
     * 1. Authenticated client domain (Origin/Referer/Host headers) against allowed domains.
     * 2. API security access token using constant-time comparison.
     */
    @Configuration
    @Order(Ordered.HIGHEST_PRECEDENCE + 1)
    public static class SecurityFilter extends OncePerRequestFilter {

        private final AppProperties appProperties;

        public SecurityFilter(AppProperties appProperties) {
            this.appProperties = appProperties;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
                throws ServletException, IOException {

            String path = request.getRequestURI();

            // Allow Swagger documentation, Actuator health, and H2 Console
            if (path.startsWith("/swagger-ui") || path.startsWith("/v3/api-docs")
                    || path.startsWith("/actuator") || path.startsWith("/h2-console")) {
                filterChain.doFilter(request, response);
                return;
            }

            String traceId = MDC.get("traceId");
            if (traceId == null) {
                traceId = (String) request.getAttribute(TRACE_ID_ATTRIBUTE);
            }

            // 1. Authenticate domain if domain check is configured
            if (appProperties.getSecurity().isDomainCheckActive()) {
                String clientDomain = DomainMatcher.extractDomain(request);
                boolean domainAllowed = DomainMatcher.isDomainAllowed(clientDomain, appProperties.getSecurity().getAllowedDomains());

                if (!domainAllowed) {
                    log.warn("Forbidden: Request from unauthenticated domain '{}' to path '{}' [traceId={}]",
                            clientDomain, path, traceId);
                    response.setStatus(HttpStatus.FORBIDDEN.value());
                    response.setContentType("application/json");
                    response.getWriter().write("{\"success\":false,\"message\":\"Forbidden: Request domain '" + clientDomain + "' is not authenticated\",\"traceId\":\"" + (traceId != null ? traceId : "") + "\"}");
                    return;
                }
            }

            // 2. Authenticate token if enabled
            if (appProperties.getSecurity().isAuthEnabled()) {
                String expectedToken = appProperties.getSecurity().getAuthToken();
                String headerName = appProperties.getSecurity().getAuthHeaderName();
                String clientToken = request.getHeader(headerName);

                // Check Bearer authorization fallback
                if (clientToken == null || clientToken.isBlank()) {
                    String authHeader = request.getHeader("Authorization");
                    if (authHeader != null && authHeader.startsWith("Bearer ")) {
                        clientToken = authHeader.substring(7);
                    }
                }

                // Constant-time comparison
                boolean isAuthenticated = false;
                if (clientToken != null) {
                    byte[] a = clientToken.getBytes(StandardCharsets.UTF_8);
                    byte[] b = expectedToken.getBytes(StandardCharsets.UTF_8);
                    isAuthenticated = MessageDigest.isEqual(a, b);
                }

                if (!isAuthenticated) {
                    log.warn("Unauthorized request to path: {} from IP: {} [traceId={}]", path, request.getRemoteAddr(), traceId);
                    response.setStatus(HttpStatus.UNAUTHORIZED.value());
                    response.setContentType("application/json");
                    response.getWriter().write("{\"success\":false,\"message\":\"Unauthorized: Invalid or missing API token\",\"traceId\":\"" + (traceId != null ? traceId : "") + "\"}");
                    return;
                }
            }

            filterChain.doFilter(request, response);
        }
    }
}
