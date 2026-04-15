package com.tongtech.cntest.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("TongLINK/Q-CN 测试接口文档")
                        .version("1.0")
                        .description("TongLINK/Q-CN 消息队列功能测试接口")
                        .contact(new Contact()
                                .name("TongTech")
                                .email("support@tongtech.com")));
    }
}
