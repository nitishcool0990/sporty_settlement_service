package com.sportygroup.settlement.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class OpenApiConfig {

    private final AppProperties appProperties;

    @Bean
    public OpenAPI customOpenAPI() {
        final String securitySchemeName = "ApiKeyAuth";

        OpenAPI openAPI = new OpenAPI()
                .info(new Info()
                        .title("SportyGroup - Sports Betting Settlement Trigger Service")
                        .description("Backend service simulating sports betting event outcome handling and bet settlement via Kafka and RocketMQ")
                        .version("1.0.0")
                        .contact(new Contact().name("SportyGroup Engineering Team"))
                        .license(new License().name("Apache 2.0")));

        if (appProperties.getSecurity().isAuthEnabled()) {
            openAPI.addSecurityItem(new SecurityRequirement().addList(securitySchemeName))
                    .components(new Components()
                            .addSecuritySchemes(securitySchemeName,
                                    new SecurityScheme()
                                            .name(appProperties.getSecurity().getAuthHeaderName())
                                            .type(SecurityScheme.Type.APIKEY)
                                            .in(SecurityScheme.In.HEADER)));
        }

        return openAPI;
    }
}
