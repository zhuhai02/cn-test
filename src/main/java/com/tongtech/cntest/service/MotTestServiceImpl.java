package com.tongtech.cntest.service;

import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.MotTestService;
import java.util.UUID;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class MotTestServiceImpl implements MotTestService {
    private static final Logger log = LoggerFactory.getLogger("PROTOCOL_TEST_LOGGER");

    private static final Marker PROTOCOL_TEST = MarkerFactory.getMarker("PROTOCOL_TEST");

    private String url;

    @Autowired
    public MotTestServiceImpl(TlqcnProperties tlqcnProperties) {
        this.url = tlqcnProperties.getMotTestConfig().getUrl();
    }

    @Override
    public String simpleTest(String topic, int qos, int msgNum) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------Mqtt协议simpleTest测试开始,测试编号[{}]----------", testNum);

        try {
            String[] brokerUrls = url.split(",");
            String clientId = "mqtt-client-" + System.currentTimeMillis();

            MqttClient client = new MqttClient(brokerUrls[0], clientId, new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            options.setKeepAliveInterval(20);
            options.setServerURIs(brokerUrls);

            // 3. 设置回调并连接
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    log.info(PROTOCOL_TEST, "Mqtt协议测试连接断开，clientId：<{}>，cause：<{}> ", clientId, cause.getMessage());
                }

                public void messageArrived(String topic, MqttMessage message) {
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            client.connect(options);

            try {
                //创建消费者
                IMqttMessageListener listener = new IMqttMessageListener() {
                    @Override
                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        System.out.println("收到消息，topic：" + topic + ", message:" + new String(message.getPayload()));
                    }
                };

                client.subscribe(topic, qos, listener);

                //生产者
                for (int i = 0; i < msgNum; i++) {
                    Date date = new Date();
                    String msg = "Mqtt协议消息，num:" + i + ",time:" + date.getTime();
                    client.publish(topic, msg.getBytes(), qos, false);
                    log.info(PROTOCOL_TEST, "mqtt协议发送消息：<{}>", msg);
                }

                Thread.sleep(1000 * 30);

                log.info(PROTOCOL_TEST, "Mqtt协议simpleTest测试完成");
            } catch (Exception e) {
                log.error(PROTOCOL_TEST, "Mqtt协议同步发送测试失败，topic：<{}>", topic, e);
            }

            client.disconnect();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Mqtt协议测试失败，topic：<{}>", topic, e);
        }

        log.info(PROTOCOL_TEST, "----------Mqtt协议simpleTest测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }
}
