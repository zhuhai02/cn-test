package com.tongtech.cntest;

import com.tongtech.cntest.config.TlqcnProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(TlqcnProperties.class)
public class CnTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(CnTestApplication.class, args);
    }

}
