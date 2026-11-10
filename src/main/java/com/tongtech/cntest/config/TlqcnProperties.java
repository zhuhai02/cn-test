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

    private RotTestConfig rotTestConfig;

    private AotTestConfig aotTestConfig;

    private KotTestConfig kotTestConfig;

    private MotTestConfig motTestConfig;

    private JmsTestConfig jmsTestConfig;


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
        private boolean enabledFailoverTest;
        private boolean enabledTransactionTest;
        private boolean enabledBatchSendTest;
        private List<Integer> batchSendSizes;
        private boolean enabledManualAckTest;
        private boolean enabledAutoDeleteSubTest;
        private boolean enabledMaxMessageSizeRejectTest;
        private boolean enabledBackpressureTest;
        private BackpressureTestConfig backpressureTestConfig;
        private int consumerNum;
        private int topicPartitionNum;
        private int msgNum;
    }

    @Data
    public static class BackpressureTestConfig {
        /** producer maxPendingMessages，越小越容易触发背压 */
        private int maxPendingMessages = 50;
        /** 单条消息字节数 */
        private int msgSize = 32768;
        /** 压测持续时长（毫秒） */
        private long testDurationMs = 60000;
        /** 消费端每条消息处理休眠（毫秒），模拟慢消费 */
        private long slowConsumeSleepMs = 200;
        /** 消费者 receiverQueueSize */
        private int receiverQueueSize = 10;
        /** 相对基线允许的堆增长上限（MB） */
        private long heapGrowthLimitMb = 128;
        /** 峰值堆不超过 maxHeap 的比例 */
        private double heapMaxUsageRatio = 0.80;
        /** 队列满时是否阻塞（true=背压阻塞，false=快速失败） */
        private boolean blockIfQueueFull = true;
        /** 发送超时（秒） */
        private int sendTimeoutSec = 30;
        /** 堆采样间隔（毫秒） */
        private long heapSampleIntervalMs = 2000;
        /** 判定为“阻塞/变慢”的 send 耗时阈值（毫秒） */
        private long blockThresholdMs = 100;
        /** 平均 send 耗时超过该值也视为背压触发（毫秒） */
        private long avgSendCostThresholdMs = 50;
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
        private String primaryHttpUrl;
        private String secondaryUrl;
        private String secondaryHttpUrl;
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

    @Data
    public static class RotTestConfig {
        private boolean enabled;
        private String url;
        private String topic;
        private String producerGroup;
        private String consumerGroup;
        private String tag;
        private int msgNum;
        private boolean enabledSyncSendTest;
        private boolean enabledAsyncSendTest;
        private boolean enabledConsumerPullTest;
        private boolean enabledConsumerPushTest;
    }
    @Data
    public static class AotTestConfig {
        private boolean enabled;
        private String url;
        private String topic;
        private int msgNum;
        private String virtualHost;
        private String routingKeys;
        private String exchangeName;
        private String queueName;
        private boolean enabledDefaulExchangeTest;
        private boolean enabledDirectExchangeTest;
        private boolean enabledFanoutExchangeTest;
        private boolean enabledTopicExchangeTest;
    }
    @Data
    public static class KotTestConfig {
        private boolean enabled;
        private String url;
        private String topic;
        private int msgNum;
        private boolean enabledSyncSendTest;
        private boolean enabledSimpleConsumerTest;
    }
    @Data
    public static class MotTestConfig {
        private boolean enabled;
        private String url;
        private String topic;
        private int msgNum;
        private int qos;
        private boolean enabledSimpleTest;
    }
    @Data
    public static class JmsTestConfig {
        private boolean enabled;
        private String brokerServiceUrl;
        private String webServiceUrl;
        private String topic;
        private int msgNum;
        private boolean enabledJms1PubSubTest;
        private boolean enabledJms1P2PTest;
        private boolean enabledJms2PubSubTest;
        private boolean enabledJms2P2PTest;
    }
}
