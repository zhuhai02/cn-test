package com.tongtech.cntest.service.api;

public interface KotTestService {

    String syncSendTest(String topic, int msgNum);

    String simpleConsumerTest(String topic, int msgNum);
}
