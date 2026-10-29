package com.sportygroup.settlement.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DomainMatcherTest {

    @Test
    @DisplayName("Should extract domain correctly from Origin header")
    void shouldExtractDomainFromOriginHeader() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Origin", "https://feed.sportybet.com:8443");

        String domain = DomainMatcher.extractDomain(request);
        assertThat(domain).isEqualTo("feed.sportybet.com");
    }

    @Test
    @DisplayName("Should extract domain correctly from Referer header")
    void shouldExtractDomainFromRefererHeader() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Referer", "http://sports-provider.internal/events/outcomes");

        String domain = DomainMatcher.extractDomain(request);
        assertThat(domain).isEqualTo("sports-provider.internal");
    }

    @Test
    @DisplayName("Should extract domain correctly from X-Source-Domain header")
    void shouldExtractDomainFromSourceDomainHeader() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("X-Source-Domain", "trusted-partner.sportygroup.com");

        String domain = DomainMatcher.extractDomain(request);
        assertThat(domain).isEqualTo("trusted-partner.sportygroup.com");
    }

    @Test
    @DisplayName("Should validate exact match and wildcard subdomains correctly")
    void shouldValidateDomainsWithWildcards() {
        List<String> allowed = List.of("sportygroup.com", "*.sportybet.com", "localhost");

        // Exact match
        assertThat(DomainMatcher.isDomainAllowed("sportygroup.com", allowed)).isTrue();
        assertThat(DomainMatcher.isDomainAllowed("localhost", allowed)).isTrue();

        // Wildcard match
        assertThat(DomainMatcher.isDomainAllowed("feed.sportybet.com", allowed)).isTrue();
        assertThat(DomainMatcher.isDomainAllowed("api.v2.sportybet.com", allowed)).isTrue();

        // Subdomain of parent domain
        assertThat(DomainMatcher.isDomainAllowed("internal.sportygroup.com", allowed)).isTrue();

        // Unauthenticated domain
        assertThat(DomainMatcher.isDomainAllowed("malicious-site.com", allowed)).isFalse();
        assertThat(DomainMatcher.isDomainAllowed("sportybet.com.attacker.com", allowed)).isFalse();
        assertThat(DomainMatcher.isDomainAllowed(null, allowed)).isFalse();
    }
}
