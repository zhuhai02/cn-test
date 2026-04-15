package com.tongtech.cntest.service.api;

public interface AotTestService {

    String defaulExchangeTest(String defaultQueue, int msgNum);

    String directExchangeTest(String virtualHost, String queueName, String exchangeName, int msgNum);

    void fanoutExchangeTest();

    void topicExchangeTest();
}
