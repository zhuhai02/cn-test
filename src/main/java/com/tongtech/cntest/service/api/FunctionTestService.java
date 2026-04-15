package com.tongtech.cntest.service.api;


import com.tongtech.tlqcn.client.api.SubscriptionType;

/**
 * 功能测试服务
 */
public interface FunctionTestService {

    /**
     * 同步发送消息测试
     * @param topic 测试的主题
     * @param msgNum 测试的消息数量
     * @return 测试日志的位置和时间
     */
    String syncSendTest(String topic, int msgNum);

    /**
     * 异步发送消息测试
     * @param topic 测试的主题
     * @param msgNum 测试的消息数量
     * @return 测试日志的位置和时间
     */
    String asyncSendTest(String topic, int msgNum);

    /**
     * 订阅类型测试
     * @param topic 测试的主题
     * @param msgNum 测试的消息数量
     * @param consumerNum 消费者数量
     * @return 测试日志的位置和时间
     */
    String subscribeTypeTest(String topic, int msgNum, int consumerNum, SubscriptionType subscriptionType);

    /**
     * 消息过滤测试
     * @param topic 测试的主题
     * @return 测试日志的位置和时间
     */
    String messageFilterTest(String topic);

    /**
     * 消息回溯测试
     * @param topic 测试的主题
     * @return 测试日志的位置和时间
     */
    String messageSeekTest(String topic);

    /**
     * 广播消费测试
     * @param topic 测试的主题
     * @return 测试日志的位置和时间
     */
    String broadcastConsumeTest(String topic);


    /**
     * 死信队列测试
     * @param topic 测试的主题
     * @return 测试日志的位置和时间
     */
    String deadLetterQueueTest(String topic);

    /**
     * 消费者重试测试
     * @param topic 测试的主题
     * @return 测试日志的位置和时间
     */
    String consumerRetryTest(String topic);

    /**
     * 延迟消息测试
     * @param topic 测试的主题
     * @param delaySeconds 延迟秒数
     * @return 测试日志的位置和时间
     */
    String delayMessageTest(String topic, long delaySeconds);

    /**
     * 定时消息测试
     * @param topic 测试的主题
     * @param timestamp 指定时间的时间戳
     * @return 测试日志的位置和时间
     */
    String scheduledMessageTest(String topic, Long timestamp);

    /**
     * 消息有序性测试
     * @param topic 测试的主题
     * @param msgNum 测试的消息数量
     * @return 测试日志的位置和时间
     */
    String messageOrderTest(String topic, int msgNum);

    /**
     * 国密消息测试
     * @param topic 测试的主题
     * @param privateKeyPath 加密私钥路径
     * @param publicKeyPath 加密公钥路径
     * @return 测试日志的位置和时间
     */
    String gmMessageTest(String topic, String privateKeyPath, String publicKeyPath);

    /**
     * 国密通信测试
     * @param topic 测试的主题
     * @return 测试日志的位置和时间
     */
    String gmTlsTest(String topic);

    /**
     * 消息优先级测试
     * @param topic 测试的主题
     * @param totalPriority 优先级总数
     * @param isAbsolutePriority 是否是绝对优先级
     * @return 测试日志的位置和时间
     */
    String messagePriorityTest(String topic, int totalPriority, boolean isAbsolutePriority);

    /**
     * 故障转移测试
     * @param topic 测试的主题
     * @param msgNum 测试的消息数量
     * @return 测试日志的位置和时间
     */
    String failoverTest(String topic, int msgNum);

    /**
     * 事务消息测试
     * @param inputTopic 输入主题
     * @param outputTopicOne 输出主题1
     * @param outputTopicTwo 输出主题2
     * @param msgNum 测试的消息数量
     * @return 测试日志的位置和时间
     */
    String transactionTest(String inputTopic, String outputTopicOne, String outputTopicTwo, int msgNum);
}
