package com.project.login.model.request.login;

import lombok.Data;

@Data
public class RegisterRequest {
    private String username;
    private String studentNumber;
    private String email;
    private String password;
    private String code; // 邮箱验证码
}

