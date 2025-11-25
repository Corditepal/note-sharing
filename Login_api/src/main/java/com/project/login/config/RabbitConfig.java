package com.project.login.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    // --- 连接工厂 ---
    @Bean
    public CachingConnectionFactory rabbitConnectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory("localhost", 5672);
        factory.setUsername("admin");
        factory.setPassword("admin");
        return factory;
    }

    // --- RabbitTemplate ---
    @Bean
    public RabbitTemplate rabbitTemplate(CachingConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jacksonMessageConverter()); // 使用 JSON
        return template;
    }

    // --- JSON 消息转换器 ---
    @Bean
    public Jackson2JsonMessageConverter jacksonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ---------------- 队列 ----------------
    @Bean
    public Queue noteRedisQueue() {
        return new Queue("note.redis.queue", true);
    }

    @Bean
    public Queue noteEsQueue() {
        return new Queue("note.es.queue", true);
    }

    @Bean
    public Queue noteMongoQueue() {
        return new Queue("note.mongo.queue", true);
    }
}
