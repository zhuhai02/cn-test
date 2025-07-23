package com.tongtech.cntest;

import com.tongtech.cntest.config.CnmqProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(CnmqProperties.class)
public class CnTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(CnTestApplication.class, args);
    }

}
