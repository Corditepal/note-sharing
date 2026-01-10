package com.project.login.controller;

import com.project.login.model.entity.UserEntity;
import com.project.login.model.request.login.EmailRequest;
import com.project.login.model.request.login.LoginRequest;
import com.project.login.model.request.login.RegisterRequest;
import com.project.login.model.request.login.ResetPasswordRequest;
import com.project.login.service.login.AuthService;
import com.project.login.service.login.EmailService;
import com.project.login.service.login.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@Slf4j
@Tag(name = "User Authentication", description = "Login, register, reset password")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/auth")
public class AuthenticationController {

    private final AuthService authService;
    private final UserService userService;
    private final EmailService emailService;

    // --- 1) 登录 ---
    @Operation(summary = "User login")
    @PostMapping("/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest request) {
        Map<String, String> token = authService.login(request);
        return ResponseEntity.ok(token);
    }

    // --- 2) 注册：发送邮箱验证码 ---
    @Operation(summary = "Send email code for registration")
    @PostMapping("/register/send-code")
    public ResponseEntity<?> sendRegisterCode(@RequestBody EmailRequest request) {
        emailService.sendRegisterCode(request.getEmail());
        return ResponseEntity.ok(Map.of("message", "验证码已发送"));
    }

    // --- 3) 注册 ---
    @Operation(summary = "Register new user")
    @PostMapping("/register")
    public ResponseEntity<?> register(@Valid @RequestBody RegisterRequest request) {
        userService.registerUser(request);
        return ResponseEntity.ok(Map.of("message", "注册成功"));
    }

    // --- 4) 忘记密码：发送验证码 ---
    @Operation(summary = "Send email code for password reset")
    @PostMapping("/password/send-code")
    public ResponseEntity<?> sendResetPasswordCode(@RequestBody EmailRequest request) {
        emailService.sendResetPasswordCode(request.getEmail());
        return ResponseEntity.ok(Map.of("message", "验证码已发送"));
    }

    // --- 5) 忘记密码：重设密码 ---
    @Operation(summary = "Reset password using email code")
    @PostMapping("/password/reset")
    public ResponseEntity<?> resetPassword(@Valid @RequestBody ResetPasswordRequest request) {
        userService.resetPassword(request);
        return ResponseEntity.ok(Map.of("message", "密码修改成功"));
    }

    // --- 6) 获取当前用户信息 ---
    @Operation(summary = "Get current logged-in user info")
    @GetMapping("/me")
    public ResponseEntity<?> getCurrentUser(
            @RequestHeader(value = "Authorization", required = false) String authorizationHeader
    ) {
        if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
            return ResponseEntity.status(401).body(Map.of("error", "用户未登录或 Token 格式错误 (需以 'Bearer ' 开头)"));
        }

        String token = authorizationHeader.substring(7);

        try {
            UserEntity user = authService.getUserByToken(token);

            if (user == null) {
                return ResponseEntity.status(401).body(Map.of("error", "Token 无效或已过期"));
            }

            Map<String, Object> result = Map.of(
                    "id", user.getId(),
                    "username", user.getUsername(),
                    "email", user.getEmail(),
                    "studentNumber", user.getStudentNumber() == null ? "" : user.getStudentNumber(), // 防止 null
                    "enabled", user.isEnabled(),
                    "avatarUrl", user.getAvatarUrl() == null ? "" : user.getAvatarUrl() // 添加头像URL
            );

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            return ResponseEntity.status(401).body(Map.of("error", "Token 解析失败"));
        }
    }

    // --- 7) 上传用户头像 ---
    @Operation(summary = "Upload user avatar")
    @PostMapping(value = "/avatar", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadAvatar(
            @RequestHeader(value = "Authorization", required = false) String authorizationHeader,
            @RequestPart("file") MultipartFile file
    ) {
        if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
            return ResponseEntity.status(401).body(Map.of("error", "用户未登录或 Token 格式错误"));
        }

        String token = authorizationHeader.substring(7);

        try {
            UserEntity user = authService.getUserByToken(token);
            if (user == null) {
                return ResponseEntity.status(401).body(Map.of("error", "Token 无效或已过期"));
            }

            String avatarUrl = userService.updateAvatar(user.getId(), file);
            return ResponseEntity.ok(Map.of("message", "头像上传成功", "avatarUrl", avatarUrl));

        } catch (RuntimeException e) {
            return ResponseEntity.status(400).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("上传头像失败", e);
            return ResponseEntity.status(500).body(Map.of("error", "上传头像失败，请稍后重试"));
        }
    }

}
