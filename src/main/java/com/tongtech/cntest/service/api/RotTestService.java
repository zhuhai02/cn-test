package com.tongtech.cntest.service.api;

public interface RotTestService {
    void startTest();

    void syncSendTest();

    void asyncSendTest();

    void consumerPullTest();

    void consumerPushTest();
}
