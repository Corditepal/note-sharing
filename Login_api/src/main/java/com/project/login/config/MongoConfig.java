package com.project.login.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.EnableMongoAuditing;

@Configuration
// 开启审计功能，这样实体类上的 @CreatedDate 和 @LastModifiedDate 才会生效
@EnableMongoAuditing
public class MongoConfig {

    // application.yml 中已经配置了 uri，
    // Spring Boot 会自动配置 MongoTemplate
    // 除非需要自定义类型转换器 (Converters) 或者配置事务管理器
}