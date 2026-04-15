package com.tongtech.cntest.config;

import java.util.List;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 配置类
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "tlqcn")
public class TlqcnProperties {

    private Client client;

    private RotTestConfig rotTestConfig;

    private AotTestConfig aotTestConfig;

    private KotTestConfig kotTestConfig;

    private MotTestConfig motTestConfig;

    private JmsTestConfig jmsTestConfig;


    @Data
    public static class Client {

        private String serviceUrl;

        private String serviceHttpUrl;

        private FailoverConfig failoverConfig;
    }


    @Data
    public static class FailoverConfig {
        private String primaryUrl;
        private String primaryHttpUrl;
        private String secondaryUrl;
        private String secondaryHttpUrl;
    }


    @Data
    public static class RotTestConfig {
        private String url;
    }
    @Data
    public static class AotTestConfig {
        private String url;
    }
    @Data
    public static class KotTestConfig {
        private String url;
    }
    @Data
    public static class MotTestConfig {
        private String url;
    }
    @Data
    public static class JmsTestConfig {
        private String brokerServiceUrl;
        private String webServiceUrl;
    }
}
