package com.tongtech.cntest.service;

import com.tongtech.cnjms.TlqcnConnectionFactory;
import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.JmsTestService;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class JmsTestServiceImpl implements JmsTestService {
    private static final Logger log = LoggerFactory.getLogger("PROTOCOL_TEST_LOGGER");

    private static final Marker PROTOCOL_TEST = MarkerFactory.getMarker("PROTOCOL_TEST");


    private String brokerServiceUrl;
    private String webServiceUrl;

    @Autowired
    public JmsTestServiceImpl(TlqcnProperties tlqcnProperties) {
        this.brokerServiceUrl = tlqcnProperties.getJmsTestConfig().getBrokerServiceUrl();
        this.webServiceUrl = tlqcnProperties.getJmsTestConfig().getWebServiceUrl();
    }

    @Override
    public String jms2P2PTest(String topic, int msgNum) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("brokerServiceUrl", brokerServiceUrl);
        properties.put("webServiceUrl", webServiceUrl);

        TlqcnConnectionFactory factory = new TlqcnConnectionFactory(properties);
        JMSContext context = factory.createContext();
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------Jms协议点对点测试开始,测试编号[{}]----------", testNum);
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
        context.close();
        log.info(PROTOCOL_TEST, "----------Jms协议点对点测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }
}
