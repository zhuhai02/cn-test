package com.tongtech.cntest.service.api;


/**
 * 功能测试服务
 */
public interface FunctionTestService {

    void startTest();

    /**
     * 同步发送消息测试
     */
    void syncSendTest();

    /**
     * 异步发送消息测试
     */
    void asyncSendTest();

    /**
     * 订阅类型测试
     */
    void subscribeTypeTest();

    /**
     * 消息过滤测试
     */
    void messageFilterTest();

    /**
     * 消息回溯测试
     */
    void messageSeekTest();

    /**
     * 广播消费测试
     */
    void broadcastConsumeTest();

    /**
     * 消息存活时间测试
     */
    void messageTtlTest();

    /**
     * 死信队列测试
     */
    void deadLetterQueueTest();

    /**
     * 消费者重试测试
     */
    void consumerRetryTest();

    /**
     * 延迟消息测试
     */
    void delayMessageTest();

    /**
     * 定时消息测试
     */
    void scheduledMessageTest();

    /**
     * 消息有序性测试
     */
    void messageOrderTest();

    /**
     * 国密消息测试
     */
    void gmMessageTest();

    /**
     * 国密通信测试
     */
    void gmTlsTest();

    /**
     * 消息优先级测试
     */
    void messagePriorityTest();
}
