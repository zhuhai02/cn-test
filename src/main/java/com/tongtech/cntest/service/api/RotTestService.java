package com.tongtech.cntest.service.api;

public interface RotTestService {

    String syncSendTest(String producerGroup, String topic, int msgNum);

    String asyncSendTest(String producerGroup, String topic, int msgNum);

    String consumerPullTest(String topic, String consumerGroup);

    String consumerPushTest(String topic, String consumerGroup, int msgNum);
}
