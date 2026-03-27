package com.tongtech.cntest.service;

import com.tongtech.cnjms.TlqcnConnectionFactory;
import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.JmsTestService;
import jakarta.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
public class JmsTestServiceImpl implements JmsTestService {
    private static final Logger log = LoggerFactory.getLogger("PROTOCOL_TEST_LOGGER");

    private static final Marker PROTOCOL_TEST = MarkerFactory.getMarker("PROTOCOL_TEST");

    private final TlqcnProperties tlqcnProperties;

    private String brokerServiceUrl;
    private String webServiceUrl;
    private String topic;
    private int msgNum = 0;
    JMSContext context;

    @Autowired
    public JmsTestServiceImpl(TlqcnProperties tlqcnProperties) {
        this.tlqcnProperties = tlqcnProperties;
        this.brokerServiceUrl = tlqcnProperties.getJmsTestConfig().getBrokerServiceUrl();
        this.webServiceUrl = tlqcnProperties.getJmsTestConfig().getWebServiceUrl();
        this.topic = tlqcnProperties.getJmsTestConfig().getTopic();
        this.msgNum = tlqcnProperties.getJmsTestConfig().getMsgNum();
    }

    @Override
    public void startTest() {
        try {
            Map<String, Object> properties = new HashMap<>();
            properties.put("brokerServiceUrl", brokerServiceUrl);
            properties.put("webServiceUrl", webServiceUrl);

            TlqcnConnectionFactory factory = new TlqcnConnectionFactory(properties);
            this.context = factory.createContext();

            if (tlqcnProperties.getJmsTestConfig().isEnabledJms2P2PTest()) {
                jms2P2PTest();
            }

            context.close();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Jms协议测试失败，topic：<{}>", topic, e);
        }
        log.info(PROTOCOL_TEST, "---------------Jms协议测试结束----------------");
    }

    @Override
    public void jms2P2PTest() {
        log.info(PROTOCOL_TEST, "Jms协议点对点测试开始");
        try {
            Queue queue = context.createQueue(topic);

            //消费者
            JMSConsumer consumer = context.createConsumer(queue);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        TextMessage textMessage = (TextMessage) message;

                        log.info(PROTOCOL_TEST, "Jms协议点对点收到消息：<{}>", textMessage.getText());
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            //生产者
            JMSProducer producer = context.createProducer();

            for (int i = 0; i < msgNum; i++) {
                Date date = new Date();
                String msg = "Jms协议消息，num:" + i + ",time:" + date.getTime();

                producer.send(queue, context.createTextMessage(msg));

                log.info(PROTOCOL_TEST, "Jms协议发送消息：<{}>", msg);

                Thread.sleep(500);
            }

            Thread.sleep(1000 * 30);

            log.info(PROTOCOL_TEST, "Jms协议点对点测试完成");
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Jms协议点对点测试失败，topic：<{}>", topic, e);
        }
    }
}
