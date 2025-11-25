package com.project.login.service.login;

import com.project.login.model.request.login.RegisterRequest;
import com.project.login.model.request.login.ResetPasswordRequest;
import com.project.login.model.entity.UserEntity;
import com.project.login.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final TokenService tokenService;
    private final PasswordEncoder passwordEncoder;

    // ----------------- 注册用户 -----------------
    public void registerUser(RegisterRequest req) {

        if (userRepository.existsByStudentNumber(req.getStudentNumber())) {
            throw new RuntimeException("学号已被注册！");
        }

        if (userRepository.existsByEmail(req.getEmail())) {
            throw new RuntimeException("邮箱已被注册！");
        }

        boolean codeValid = tokenService.validate(req.getEmail(), req.getCode(), "REGISTER");
        if (!codeValid) {
            throw new RuntimeException("验证码无效或已过期");
        }

        UserEntity user = new UserEntity();
        user.setUsername(req.getUsername());
        user.setStudentNumber(req.getStudentNumber());
        user.setEmail(req.getEmail());
        user.setPassword_hash(passwordEncoder.encode(req.getPassword()));
        user.setEnabled(true);

        userRepository.save(user);
    }

    // ----------------- 重设密码 -----------------
    public void resetPassword(ResetPasswordRequest req) {

        UserEntity user = userRepository.findByEmail(req.getEmail())
                .orElseThrow(() -> new RuntimeException("邮箱不存在"));

        boolean valid = tokenService.validate(req.getEmail(), req.getCode(), "RESET");
        if (!valid) {
            throw new RuntimeException("验证码无效或已过期");
        }

        user.setPassword_hash(passwordEncoder.encode(req.getNewPassword()));
        userRepository.save(user);
    }


}

