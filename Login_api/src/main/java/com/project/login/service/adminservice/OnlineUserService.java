package com.project.login.service.adminservice;

import com.project.login.model.entity.UserEntity;
import com.project.login.service.login.JwtService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OnlineUserService {

    private final RabbitTemplate rabbitTemplate;
    private final RabbitAdmin rabbitAdmin;
    private final JwtService jwtService;
    private static final String ACTIVE_TOKEN_QUEUE = "active.token.queue";

    public void addActiveToken(String token) {
        try {
            String role = jwtService.extractRole(token);
            if (role != null && "Admin".equals(role)) {
                log.debug("管理员 token 不添加到队列");
                return;
            }
            
            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setExpiration("3600000");
            
            Message message = new Message(token.getBytes(StandardCharsets.UTF_8), messageProperties);
            rabbitTemplate.send(ACTIVE_TOKEN_QUEUE, message);
            
            log.debug("Token 已添加到活跃队列: {}", token.substring(0, Math.min(20, token.length())) + "...");
        } catch (Exception e) {
            log.error("添加活跃 token 失败", e);
        }
    }

    public void removeActiveToken(String token) {
        try {
            // 注意：RabbitMQ 不支持直接删除特定消息
            // 这里只是记录日志，实际可以通过消费者来处理
            log.debug("Token 应从活跃队列中移除: {}", token.substring(0, Math.min(20, token.length())) + "...");
        } catch (Exception e) {
            log.error("移除活跃 token 失败", e);
        }
    }

    public List<UserEntity> getAllOnlineUsers() {
        List<UserEntity> onlineUsers = new ArrayList<>();
        List<String> validTokens = new ArrayList<>();
        
        try {
            Queue queue = new Queue(ACTIVE_TOKEN_QUEUE, true);
            rabbitAdmin.declareQueue(queue);
            
            Message message;
            int maxIterations = 10000;
            int count = 0;
            
            while ((message = rabbitTemplate.receive(ACTIVE_TOKEN_QUEUE, 100)) != null && count < maxIterations) {
                count++;
                
                try {
                    String token = new String(message.getBody(), StandardCharsets.UTF_8);
                    
                    String role = jwtService.extractRole(token);
                    if (role != null && "Admin".equals(role)) {
                        continue;
                    }
                    
                    UserEntity user = jwtService.getUserByToken(token);
                    if (user != null) {
                        boolean isDuplicate = onlineUsers.stream()
                                .anyMatch(u -> u.getId().equals(user.getId()));
                        
                        if (!isDuplicate) {
                            onlineUsers.add(user);
                        }
                        
                        validTokens.add(token);
                    }
                } catch (Exception e) {
                    log.warn("解析 token 失败，跳过该消息", e);
                }
            }
            
            // 将有效的 token 重新发送回队列（保持队列状态）
            for (String token : validTokens) {
                addActiveToken(token);
            }
            
            log.info("获取到 {} 个在线用户", onlineUsers.size());
            return onlineUsers;
            
        } catch (Exception e) {
            log.error("获取在线用户列表失败", e);
            return onlineUsers; // 返回已获取的用户列表
        }
    }

    public List<Map<String, Object>> getAllOnlineUsersWithLoginTime() {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<Long, UserEntity> userMap = new HashMap<>(); // 存储用户信息
        Map<Long, Date> userLoginTimeMap = new HashMap<>(); // 存储每个用户的最早上线时间
        List<String> validTokens = new ArrayList<>();
        
        try {
            Queue queue = new Queue(ACTIVE_TOKEN_QUEUE, true);
            rabbitAdmin.declareQueue(queue);
            
            Message message;
            int maxIterations = 10000;
            int count = 0;
            
            while ((message = rabbitTemplate.receive(ACTIVE_TOKEN_QUEUE, 100)) != null && count < maxIterations) {
                count++;
                
                try {
                    String token = new String(message.getBody(), StandardCharsets.UTF_8);
                    
                    String role = jwtService.extractRole(token);
                    if (role != null && "Admin".equals(role)) {
                        continue;
                    }
                    
                    UserEntity user = jwtService.getUserByToken(token);
                    if (user != null) {
                        // 提取TOKEN的签发时间（上线时间）
                        Date loginTime = jwtService.extractIssuedAt(token);
                        
                        // 存储用户信息
                        userMap.put(user.getId(), user);
                        
                        // 如果该用户已存在，取最早的登录时间
                        if (userLoginTimeMap.containsKey(user.getId())) {
                            Date existingTime = userLoginTimeMap.get(user.getId());
                            if (loginTime != null && loginTime.before(existingTime)) {
                                userLoginTimeMap.put(user.getId(), loginTime);
                            }
                        } else {
                            // 新用户，直接添加
                            if (loginTime != null) {
                                userLoginTimeMap.put(user.getId(), loginTime);
                            }
                        }
                        
                        validTokens.add(token);
                    }
                } catch (Exception e) {
                    log.warn("解析 token 失败，跳过该消息", e);
                }
            }
            
            // 将有效的 token 重新发送回队列（保持队列状态）
            for (String token : validTokens) {
                addActiveToken(token);
            }
            
            // 构建最终结果
            for (Map.Entry<Long, UserEntity> entry : userMap.entrySet()) {
                Map<String, Object> userInfo = new HashMap<>();
                UserEntity user = entry.getValue();
                userInfo.put("user", user);
                userInfo.put("loginTime", userLoginTimeMap.get(entry.getKey()));
                result.add(userInfo);
            }
            
            log.info("获取到 {} 个在线用户（含上线时间）", result.size());
            return result;
            
        } catch (Exception e) {
            log.error("获取在线用户列表失败", e);
            return result;
        }
    }
}
