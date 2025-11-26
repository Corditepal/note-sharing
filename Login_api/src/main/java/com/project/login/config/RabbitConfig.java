package com.project.login.config;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.core.Queue;


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
    /** * 使用 JSON 序列化机制，进行消息转换 * 自动将 Pojo 对象转换为 Json 格式发送，接收时自动解析 */
    @Bean
    public Jackson2JsonMessageConverter jacksonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // 在 yml 中配置了 listener.simple.acknowledge-mode: manual
    // Spring Boot 自动配置会读取该属性，无需手动创建 ContainerFactory
    // 只需要在 @RabbitListener 代码中手动调用 channel.basicAck 即可

    // ---------------- 队列 ----------------
    @Bean
    public Queue noteIndexQueue() {
        return new Queue("note_index_queue", true);
    }

    @Bean
    public Queue noteStatsQueue() {
        return new Queue("note_stats_queue", true);
    }

    @Bean
    public Queue noteFavorQueue() {
        return new Queue("note_favor_queue", true);
    }
}
