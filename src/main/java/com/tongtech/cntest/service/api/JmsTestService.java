package com.tongtech.cntest.service.api;

public interface JmsTestService {

    /**
     * jms协议点对点测试
     * @param topic 主题
     * @param msgNum 消息数量
     * @return
     */
    String jms2P2PTest(String topic, int msgNum);
}
