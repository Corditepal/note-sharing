package com.project.login.service.login;

import com.project.login.model.request.login.RegisterRequest;
import com.project.login.model.request.login.ResetPasswordRequest;
import com.project.login.model.entity.UserEntity;
import com.project.login.mapper.UserMapper;
import com.project.login.repository.UserRepository;
import com.project.login.service.minio.MinioService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final UserMapper userMapper;
    private final TokenService tokenService;
    private final PasswordEncoder passwordEncoder;
    private final MinioService minioService;

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

    // ----------------- 更新用户头像 -----------------
    public String updateAvatar(Long userId, MultipartFile file) {
        // 1. 验证文件类型（只允许图片）
        String contentType = file.getContentType();
        if (contentType == null || !contentType.startsWith("image/")) {
            throw new RuntimeException("只能上传图片文件");
        }

        // 2. 验证文件大小（限制为5MB）
        long maxSize = 5 * 1024 * 1024; // 5MB
        if (file.getSize() > maxSize) {
            throw new RuntimeException("图片大小不能超过5MB");
        }

        // 3. 上传文件到MinIO
        String fileName = minioService.uploadFile(file);
        String avatarUrl = minioService.getFileUrl(fileName);

        // 4. 验证用户存在
        userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("用户不存在"));

        // 5. 更新数据库中的头像URL
        userMapper.updateAvatarUrl(userId, avatarUrl);

        return avatarUrl;
    }


}

