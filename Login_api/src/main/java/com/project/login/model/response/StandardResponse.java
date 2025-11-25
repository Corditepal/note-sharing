package com.project.login.model.response;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StandardResponse<T> {
    private Integer code;
    private String message;
    private T data;

    public static <T> StandardResponse<T> success(T data) {
        return StandardResponse.<T>builder()
                .code(200)
                .message("success")
                .data(data)
                .build();
    }

    public static <T> StandardResponse<T> success(String message, T data) {
        return StandardResponse.<T>builder()
                .code(200)
                .message(message)
                .data(data)
                .build();
    }
}
