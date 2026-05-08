package com.tongtech.cntest.service.api;

public interface JmsTestService {
    void startTest();

    void jms1P2PTest();

    void jms1PubSubTest();

    void jms2P2PTest();

    void jms2PubSubTest();
}
