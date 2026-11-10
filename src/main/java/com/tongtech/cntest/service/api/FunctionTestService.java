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

    void failoverTest();

    /**
     * 事务消息测试
     */
    void transactionTest();

    /**
     * 批量发送消息测试
     */
    void batchSendTest();

    /**
     * 手动ACK/NACK测试：ACK后删除消息，NACK后重新入队
     */
    void manualAckTest();

    /**
     * 自动删除订阅测试：NonDurable 订阅在最后消费者断开后自动删除
     */
    void autoDeleteSubTest();

    /**
     * 消息超长拒绝测试：超过 maxMessageSize 的消息按拒绝策略处理
     */
    void maxMessageSizeRejectTest();

    /**
     * 背压测试：生产超过处理速率时触发背压，系统稳定不OOM
     */
    void backpressureTest();
}
