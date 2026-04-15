package com.tongtech.cntest.service.api;

public interface PerfTestService {

    String startTest(String topic, int topicPartitionNum, int msgSize,
                   boolean isReCreateTopic, boolean isSyncSend,
                   int producerNum, int consumerNum, int testMinutes);

}
