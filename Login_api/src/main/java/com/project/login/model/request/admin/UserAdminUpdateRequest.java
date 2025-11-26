package com.project.login.model.request.admin;

import lombok.Data;

@Data
public class UserAdminUpdateRequest {
    private String username;
    private String studentNumber;
    private String email;
}