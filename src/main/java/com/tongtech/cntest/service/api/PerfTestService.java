package com.tongtech.cntest.service.api;

public interface PerfTestService {

    void startTest();

    /**
     * 同步性能测试
     * @param producerNum 生产者数量
     * @param consumerNum 消费者数量
     * @param topicPartitionNum 主题分区数量
     * @param msgSize 消息大小
     * @param perProducerThreadNum 每个生产者线程数量
     * @param consumeAfterSend 是否先发送再消费
     */
    void syncPerfTest(int producerNum, int consumerNum, int topicPartitionNum,
                      int msgSize, int perProducerThreadNum, boolean consumeAfterSend, boolean syncAck);

    /**
     * 异步性能测试
     * @param producerNum 生产者数量
     * @param consumerNum 消费者数量
     * @param topicPartitionNum 主题分区数量
     * @param msgSize 消息大小
     * @param perProducerThreadNum 每个生产者线程数量
     * @param consumeAfterSend 是否先发送再消费
     */
    void asyncPerfTest(int producerNum, int consumerNum, int topicPartitionNum,
                       int msgSize, int perProducerThreadNum, boolean consumeAfterSend, boolean syncAck);

}
