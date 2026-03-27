package com.tongtech.cntest.service.api;

public interface AotTestService {
    void startTest();

    void defaulExchangeTest();

    void directExchangeTest();

    void fanoutExchangeTest();

    void topicExchangeTest();
}
