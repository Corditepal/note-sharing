package com.project.login.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.Data;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "elasticsearch") // 读取 yml 中的 elasticsearch 配置
public class ElasticsearchConfig {

    private String host;
    private Integer port;
    private String scheme;

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        // 1. 创建低级客户端
        RestClient restClient = RestClient.builder(
                new HttpHost(host, port, scheme)
        ).build();

        // 2. 创建传输层 (使用 Jackson 映射器)
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // 3. 创建 API 客户端
        return new ElasticsearchClient(transport);
    }
}