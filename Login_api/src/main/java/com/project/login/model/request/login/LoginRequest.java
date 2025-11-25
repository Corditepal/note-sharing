package com.project.login.model.request.login;

import lombok.Data;

@Data
public class LoginRequest {
    private String email;          // 可选
    private String studentNumber;  // 可选
    private String username;       // 可选
    private String password;       // 必填
}
