package com.tongtech.cntest.service;

import com.rabbitmq.client.*;
import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.AotTestService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class AotTestServiceImpl implements AotTestService {
    private static final Logger log = LoggerFactory.getLogger("PROTOCOL_TEST_LOGGER");

    private static final Marker PROTOCOL_TEST = MarkerFactory.getMarker("PROTOCOL_TEST");

    private final TlqcnProperties tlqcnProperties;

    private String url;

    private Address[] addresses;

    private String host;

    private int port = 0;

    private String virtualHost;

    private String[] routingKeys;

    private String exchangeName;

    private String queueName;

    private String topic;

    private int msgNum = 0;

    @Autowired
    public AotTestServiceImpl(TlqcnProperties tlqcnProperties) {
        this.tlqcnProperties = tlqcnProperties;
        this.url = tlqcnProperties.getAotTestConfig().getUrl();
        this.virtualHost = tlqcnProperties.getAotTestConfig().getVirtualHost();

        String[] urlList = this.url.split(",");

        if (urlList.length == 1) {
            String[] hosts = urlList[0].split(":");
            this.host = hosts[0];
            this.port = Integer.parseInt(hosts[1]);
        } else {
            this.addresses = Arrays.stream(urlList)
                    .map(s -> s.split(":"))
                    .map(parts -> new Address(parts[0], Integer.parseInt(parts[1])))
                    .toArray(Address[]::new);
        }

        this.topic = tlqcnProperties.getAotTestConfig().getTopic();
        this.msgNum = tlqcnProperties.getAotTestConfig().getMsgNum();
        this.routingKeys = tlqcnProperties.getAotTestConfig().getRoutingKeys().split(",");
        this.exchangeName = tlqcnProperties.getAotTestConfig().getExchangeName();
        this.queueName = tlqcnProperties.getAotTestConfig().getQueueName();
    }


    @Override
    public void startTest() {
        try {


            if (tlqcnProperties.getAotTestConfig().isEnabledDefaulExchangeTest()) {
                defaulExchangeTest();
            }

            if (tlqcnProperties.getAotTestConfig().isEnabledDirectExchangeTest()) {
                directExchangeTest();
            }

            if (tlqcnProperties.getAotTestConfig().isEnabledFanoutExchangeTest()) {
                fanoutExchangeTest();
            }

            if (tlqcnProperties.getAotTestConfig().isEnabledTopicExchangeTest()) {
                topicExchangeTest();
            }


        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "AMQP协议测试失败，topic：<{}>", topic, e);
        }

        log.info(PROTOCOL_TEST, "---------------AMQP协议测试结束----------------");
    }

    private Connection createConnection(String virtualHost) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        if(StringUtils.isNotBlank(virtualHost)){
            factory.setVirtualHost(virtualHost);
        }
        factory.setRequestedHeartbeat(1000);
        // 可选：启用自动恢复，提高容错性
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);

        Connection connection = null;

        if (addresses != null && addresses.length > 1) {
            connection = factory.newConnection(addresses);
        } else {
            factory.setHost(host);
            factory.setPort(port);
            connection = factory.newConnection();
        }

        connection.addShutdownListener((ShutdownSignalException cause) -> {
            log.debug(PROTOCOL_TEST, "连接关闭，原因: {}", cause.getMessage());
            if (cause.getReason() != null) {
                log.debug(PROTOCOL_TEST, "详细原因: {}", cause.getReason());
            }
        });

        return connection;
    }

    @Override
    public void defaulExchangeTest() {
        log.info(PROTOCOL_TEST, "AMQP协议defaulExchange测试开始");

        try {
            Connection connection = createConnection(null);

            Channel channel = connection.createChannel();
            String defaultQueue = this.queueName + "_default";
            channel.queueDeclare(defaultQueue, false, false, false, null);

            //创建消费者
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                log.info(PROTOCOL_TEST, "AMQP协议defaulExchange消费成功：<{}>", message);
            };

            // 开始消费
            channel.basicConsume(defaultQueue, true, deliverCallback, consumerTag -> {
            });

            //发送消息
            for (int i = 1; i <= msgNum; i++) {
                Date date = new Date();

                String message = "AMQP协议消息，num:" + i + ",time:" + date.getTime();
                // 发送持久化消息
                channel.basicPublish("", defaultQueue, null, message.getBytes());

                log.info(PROTOCOL_TEST, "AMQP协议efaulExchange发送：<{}>", message);
            }

            Thread.sleep(1000 * 30);

            channel.close();
            connection.close();
            log.info(PROTOCOL_TEST, "AMQP协议defaulExchange测试完成");
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "AMQP协议defaulExchange测试失败，topic：<{}>", topic, e);
        }
    }

    @Override
    public void directExchangeTest() {
        log.info(PROTOCOL_TEST, "AMQP协议directExchange测试开始");

        try {
            Connection connection = createConnection(virtualHost);

            String directQueue = this.queueName + "_direct";
            String directExchange = this.exchangeName + "_direct";
            String directRoutingKey = "direct_routing_key";

            Channel channel = connection.createChannel();
            channel.basicQos(5);
            // 声明持久化直连交换机
            channel.exchangeDeclare(directExchange, "direct", true);
            // 声明持久化队列
            channel.queueDeclare(directQueue, true, false, false, null);
            // 绑定队列到交换机
            channel.queueBind(directQueue, directExchange, directRoutingKey);

            // 启用发布者确认
            channel.confirmSelect();

            //创建消费者
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    log.info(PROTOCOL_TEST, "AMQP协议directExchange消费成功：<{}>", message);

                    // 模拟处理时间
                    Thread.sleep(1000);

                    // 手动确认消息
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // 拒绝消息并重新入队
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    log.error(PROTOCOL_TEST, "AMQP协议directExchange消息消费失败，消息重新入队，topic：<{}>", topic, e);
                } catch (Exception e) {
                    //测试发现rop支持消息的重新投递
                    // 拒绝消息并重新入队
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    log.error(PROTOCOL_TEST, "AMQP协议directExchange消息消费失败，消息重新投递，topic：<{}>", topic, e);
                }
            };

            // 开始消费
            channel.basicConsume(directQueue, false, deliverCallback, consumerTag -> {
            });

            //创建生产者发送消息
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

            builder.expiration(String.valueOf("1000"));
            builder.contentType("text/plain");

            for (int i = 1; i <= msgNum; i++) {
                Date date = new Date();

                String message = "AMQP协议消息，num:" + i + ",time:" + date.getTime();

                // 发送持久化消息
                channel.basicPublish(
                        directExchange,
                        directRoutingKey,
                        builder.build(),
                        message.getBytes()
                );

                log.info(PROTOCOL_TEST, "AMQP协议directExchange机发送：<{}>", message);

                // 等待确认（可选）
                channel.waitForConfirmsOrDie(5000);
            }

            Thread.sleep(1000 * 30);

            channel.close();
            connection.close();
            log.info(PROTOCOL_TEST, "AMQP协议directExchange测试完成");
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "AMQP协议directExchange测试失败，topic：<{}>", topic, e);
        }
    }

    @Override
    public void fanoutExchangeTest() {
        log.info(PROTOCOL_TEST, "AMQP协议fanoutExchange测试开始");

        try {
            Connection connection = createConnection(virtualHost);

            String fanoutQueue = this.queueName + "_fanout";
            String fanoutExchange = this.exchangeName + "_fanout";

            Channel channel = connection.createChannel();
            channel.basicQos(5);
            // 声明持久化扇形交换机
            channel.exchangeDeclare(fanoutExchange, "fanout", true);
            // 声明持久化队列
            channel.queueDeclare(fanoutQueue, true, false, false, null);
            // 绑定队列到交换机（fanout类型不需要routingKey）
            channel.queueBind(fanoutQueue, fanoutExchange, "");

            // 启用发布者确认
            channel.confirmSelect();

            //创建消费者
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    log.info(PROTOCOL_TEST, "AMQP协议fanoutExchange消费成功：<{}>", message);

                    // 模拟处理时间
                    Thread.sleep(1000);

                    // 手动确认消息
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // 拒绝消息并重新入队
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    log.error(PROTOCOL_TEST, "AMQP协议fanoutExchange消息消费失败，消息重新入队，topic：<{}>", topic, e);
                } catch (Exception e) {
                    //测试发现rop支持消息的重新投递
                    // 拒绝消息并重新入队
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    log.error(PROTOCOL_TEST, "AMQP协议fanoutExchange消息消费失败，消息重新投递，topic：<{}>", topic, e);
                }
            };

            // 开始消费
            channel.basicConsume(fanoutQueue, false, deliverCallback, consumerTag -> {
            });

            //创建生产者发送消息
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

            builder.expiration(String.valueOf("1000"));
            builder.contentType("text/plain");

            for (int i = 1; i <= msgNum; i++) {
                Date date = new Date();

                String message = "AMQP协议消息，num:" + i + ",time:" + date.getTime();

                // 发送持久化消息（fanout类型routingKey为空）
                channel.basicPublish(
                        fanoutExchange,
                        "",
                        builder.build(),
                        message.getBytes()
                );

                log.info(PROTOCOL_TEST, "AMQP协议fanoutExchange机发送：<{}>", message);

                // 等待确认（可选）
                channel.waitForConfirmsOrDie(5000);
            }

            Thread.sleep(1000 * 30);

            channel.close();
            connection.close();
            log.info(PROTOCOL_TEST, "AMQP协议fanoutExchange测试完成");
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "AMQP协议fanoutExchange测试失败，topic：<{}>", topic, e);
        }
    }

    @Override
    public void topicExchangeTest() {
        log.info(PROTOCOL_TEST, "AMQP协议topicExchange测试开始");

        try {
            Connection connection = createConnection(virtualHost);

            String topicQueue = this.queueName + "_topic";
            String topicExchange = this.exchangeName + "_topic";

            Channel channel = connection.createChannel();
            channel.basicQos(5);
            // 声明持久化主题交换机
            channel.exchangeDeclare(topicExchange, "topic", true);
            // 声明持久化队列
            channel.queueDeclare(topicQueue, true, false, false, null);

            // 使用routingKeys配置绑定队列到交换机
            for (String routingKey : routingKeys) {
                channel.queueBind(topicQueue, topicExchange, routingKey);
            }

            // 启用发布者确认
            channel.confirmSelect();

            //创建消费者
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    log.info(PROTOCOL_TEST, "AMQP协议topicExchange消费成功：<{}>", message);

                    // 模拟处理时间
                    Thread.sleep(1000);

                    // 手动确认消息
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // 拒绝消息并重新入队
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    log.error(PROTOCOL_TEST, "AMQP协议topicExchange消息消费失败，消息重新入队，topic：<{}>", topic, e);
                } catch (Exception e) {
                    //测试发现rop支持消息的重新投递
                    // 拒绝消息并重新入队
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    log.error(PROTOCOL_TEST, "AMQP协议topicExchange消息消费失败，消息重新投递，topic：<{}>", topic, e);
                }
            };

            // 开始消费
            channel.basicConsume(topicQueue, false, deliverCallback, consumerTag -> {
            });

            //创建生产者发送消息
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

            builder.expiration(String.valueOf("1000"));
            builder.contentType("text/plain");

            for (int i = 1; i <= msgNum; i++) {
                Date date = new Date();

                String message = "AMQP协议消息，num:" + i + ",time:" + date.getTime();

                // 使用第一个routingKey发送消息
                String routingKey = routingKeys[0];

                // 发送持久化消息
                channel.basicPublish(
                        topicExchange,
                        routingKey,
                        builder.build(),
                        message.getBytes()
                );

                log.info(PROTOCOL_TEST, "AMQP协议topicExchange机发送：<{}>", message);

                // 等待确认（可选）
                channel.waitForConfirmsOrDie(5000);
            }

            Thread.sleep(1000 * 30);

            channel.close();
            connection.close();
            log.info(PROTOCOL_TEST, "AMQP协议topicExchange测试完成");
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "AMQP协议topicExchange测试失败，topic：<{}>", topic, e);
        }
    }
}
