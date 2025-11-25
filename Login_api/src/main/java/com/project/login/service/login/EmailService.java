package com.project.login.service.login;

import org.springframework.beans.factory.annotation.Value; // 导入 @Value
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;

@Service
@RequiredArgsConstructor
public class EmailService {

    private final JavaMailSender mailSender;
    private final TokenService tokenService; // 注入 TokenService

    // 关键修改：通过 @Value 注入配置文件中的发件人邮箱（即 spring.mail.username）
    @Value("${spring.mail.username}")
    private String senderEmail;

    // Controller 不改，仍然只传 email
    public void sendRegisterCode(String email) {
        // 1. 内部生成验证码
        String code = tokenService.createToken(email, "REGISTER");

        try {
            MimeMessage message = mailSender.createMimeMessage();
            // 确保 MimeMessageHelper 支持 UTF-8 编码
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            // 关键修改：设置发件人地址，解决 553 错误
            helper.setFrom(senderEmail);

            helper.setTo(email);
            helper.setSubject("注册验证码");
            helper.setText("您的注册验证码是：" + code + "，有效期5分钟");

            mailSender.send(message);
            System.out.println("验证码邮件已发送到：" + email + ", 验证码：" + code);

        } catch (MessagingException e) {
            e.printStackTrace();
            throw new RuntimeException("邮件发送失败：" + e.getMessage());
        }
    }

    // 可扩展：重置密码验证码
    public void sendResetPasswordCode(String email) {
        String code = tokenService.createToken(email, "RESET");

        try {
            MimeMessage message = mailSender.createMimeMessage();
            // 确保 MimeMessageHelper 支持 UTF-8 编码
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            // 关键修改：设置发件人地址，解决 553 错误
            helper.setFrom(senderEmail);

            helper.setTo(email);
            helper.setSubject("重置密码验证码");
            helper.setText("您的验证码是：" + code + "，有效期5分钟");

            mailSender.send(message);
            System.out.println("重置密码验证码邮件已发送到：" + email + ", 验证码：" + code);

        } catch (MessagingException e) {
            e.printStackTrace();
            throw new RuntimeException("邮件发送失败：" + e.getMessage());
        }
    }
}