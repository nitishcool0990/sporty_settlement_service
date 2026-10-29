package com.sportygroup.settlement.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiResponse<T> {
    private boolean success;
    private String message;
    private T data;
    private String traceId;
    @Builder.Default
    private Instant timestamp = Instant.now();

    public static <T> ApiResponse<T> ok(String message, T data, String traceId) {
        return ApiResponse.<T>builder()
                .success(true)
                .message(message)
                .data(data)
                .traceId(traceId)
                .timestamp(Instant.now())
                .build();
    }

    public static <T> ApiResponse<T> error(String message, String traceId) {
        return ApiResponse.<T>builder()
                .success(false)
                .message(message)
                .data(null)
                .traceId(traceId)
                .timestamp(Instant.now())
                .build();
    }
}
