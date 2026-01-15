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
    
    private FunctionTestConfig functionTestConfig;

    private PerfParamConfig perfParamConfigs;

    private List<PerfCaseConfig> perfCaseConfigs;


    @Data
    public static class Client {

        private String serviceUrl;

        private String serviceHttpUrl;

        private Authentication authentication;

        private Integer inThreads;

        private Integer listenerThreads;

        private FailoverConfig failoverConfig;
    }

    @Data
    public static class FunctionTestConfig {
        private boolean enabled;
        private String topic;
        private String publicKeyPath;
        private String privateKeyPath;
        private boolean enabledSyncSendTest;
        private boolean enabledAsyncSendTest;
        private boolean enabledSubscribeTypeTest;
        private boolean enabledMessageFilterTest;
        private boolean enabledMessageSeekTest;
        private boolean enabledBroadcastConsumeTest;
        private boolean enabledMessageTtlTest;
        private boolean enabledDeadLetterQueueTest;
        private boolean enabledConsumerRetryTest;
        private boolean enabledDelayMessageTest;
        private boolean enabledScheduledMessageTest;
        private boolean enabledMessageOrderTest;
        private boolean enabledGmMessageTest;
        private boolean enabledGmTlsTest;
        private boolean enabledMessagePriorityTest;
    }

    @Data
    public static class PerfParamConfig {
        private boolean enabled;
        private boolean enablePerfSyncTest;
        private boolean enablePerfAsyncTest;
        private String topic;
        private long batchingMaxPublishDelay;
        private int batchingMaxMessages;
        private long testTimePerCase;
        private long logInterval;
        private long caseInterval;
        private long closeTime;
    }

    @Data
    public static class Authentication {
        private boolean enabled;
        private String token;
    }

    @Data
    public static class FailoverConfig {
        private String primaryUrl;
        private String secondaryUrl;
        private long delay;
        private long switchBackDelay;
        private long checkInterval;
        private String checkTopic;
    }

    @Data
    public static class PerfCaseConfig {
        private int producerNum;
        private int consumerNum;
        private int topicPartitionNum;
        private int msgSize;
        private int perProducerThreadNum;
        private boolean consumeAfterSend;
        private boolean syncAck;
    }

}
