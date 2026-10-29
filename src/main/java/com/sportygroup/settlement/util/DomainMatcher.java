package com.sportygroup.settlement.util;

import jakarta.servlet.http.HttpServletRequest;

import java.net.URI;
import java.util.List;

public final class DomainMatcher {

    private DomainMatcher() {}

    /**
     * Extracts the client domain/host from request headers (Origin, Referer, X-Source-Domain, Host).
     */
    public static String extractDomain(HttpServletRequest request) {
        String origin = request.getHeader("Origin");
        if (origin != null && !origin.isBlank()) {
            String host = extractHostFromUrl(origin);
            if (host != null && !host.isBlank()) return host;
        }

        String referer = request.getHeader("Referer");
        if (referer != null && !referer.isBlank()) {
            String host = extractHostFromUrl(referer);
            if (host != null && !host.isBlank()) return host;
        }

        String sourceDomain = request.getHeader("X-Source-Domain");
        if (sourceDomain != null && !sourceDomain.isBlank()) {
            return sourceDomain.trim().toLowerCase();
        }

        String forwardedHost = request.getHeader("X-Forwarded-Host");
        if (forwardedHost != null && !forwardedHost.isBlank()) {
            // Might be comma-separated or contain port
            String firstHost = forwardedHost.split(",")[0].trim();
            return stripPort(firstHost).toLowerCase();
        }

        String hostHeader = request.getHeader("Host");
        if (hostHeader != null && !hostHeader.isBlank()) {
            return stripPort(hostHeader).toLowerCase();
        }

        return request.getRemoteHost() != null ? request.getRemoteHost().toLowerCase() : "";
    }

    /**
     * Validates if the extracted domain matches any of the configured allowed domain patterns.
     * Supports exact match and wildcard subdomains (e.g. "*.sportygroup.com").
     */
    public static boolean isDomainAllowed(String domain, List<String> allowedPatterns) {
        if (allowedPatterns == null || allowedPatterns.isEmpty()) {
            return true;
        }
        if (domain == null || domain.isBlank()) {
            return false;
        }

        String cleanDomain = domain.toLowerCase();

        for (String pattern : allowedPatterns) {
            if (pattern == null || pattern.isBlank()) continue;
            String cleanPattern = pattern.trim().toLowerCase();

            // Match exact
            if (cleanPattern.equals(cleanDomain)) {
                return true;
            }

            // Match wildcard, e.g. *.sportygroup.com or *.sportybet.com
            if (cleanPattern.startsWith("*.")) {
                String suffix = cleanPattern.substring(1); // .sportygroup.com
                if (cleanDomain.endsWith(suffix) && cleanDomain.length() > suffix.length()) {
                    return true;
                }
            } else if (cleanDomain.endsWith("." + cleanPattern)) {
                // If allowed domain is sportygroup.com, also allow feed.sportygroup.com
                return true;
            }
        }
        return false;
    }

    private static String extractHostFromUrl(String url) {
        try {
            URI uri = URI.create(url);
            String host = uri.getHost();
            if (host != null) {
                return host.toLowerCase();
            }
        } catch (Exception ignored) {
        }
        return stripPort(url.replaceFirst("^https?://", "").split("/")[0]).toLowerCase();
    }

    private static String stripPort(String host) {
        int colonIdx = host.indexOf(':');
        return colonIdx != -1 ? host.substring(0, colonIdx) : host;
    }
}
