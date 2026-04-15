package com.tongtech.cntest.service;

import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.AdminService;
import com.tongtech.cntest.service.api.FunctionTestService;
import com.tongtech.cntest.utils.PriorityUtil;
import com.tongtech.cntest.utils.RawFileKeyReader;
import com.tongtech.tlqcn.client.MessageCryptoGm4Consumer;
import com.tongtech.tlqcn.client.MessageCryptoGm4Producer;
import com.tongtech.tlqcn.client.admin.TlqcnAdmin;
import com.tongtech.tlqcn.client.admin.TlqcnAdminException;
import com.tongtech.tlqcn.client.api.Consumer;
import com.tongtech.tlqcn.client.api.ConsumerBuilder;
import com.tongtech.tlqcn.client.api.DeadLetterPolicy;
import com.tongtech.tlqcn.client.api.Message;
import com.tongtech.tlqcn.client.api.MessageId;
import com.tongtech.tlqcn.client.api.MessageListener;
import com.tongtech.tlqcn.client.api.Producer;
import com.tongtech.tlqcn.client.api.ProducerBuilder;
import com.tongtech.tlqcn.client.api.Reader;
import com.tongtech.tlqcn.client.api.Schema;
import com.tongtech.tlqcn.client.api.ServiceUrlProvider;
import com.tongtech.tlqcn.client.api.SubscriptionInitialPosition;
import com.tongtech.tlqcn.client.api.SubscriptionType;
import com.tongtech.tlqcn.client.api.TlqcnClient;
import com.tongtech.tlqcn.client.api.TlqcnClientException;
import com.tongtech.tlqcn.client.api.transaction.Transaction;
import com.tongtech.tlqcn.client.impl.AutoClusterFailover;
import com.tongtech.tlqcn.shade.com.google.common.collect.Lists;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FunctionTestServiceImpl implements FunctionTestService {
    private static final Logger log = LoggerFactory.getLogger("FUNCTION_TEST_LOGGER");
    private static final Marker FUNCTION_TEST = MarkerFactory.getMarker("FUNCTION_TEST");

    private final TlqcnClient tlqcnClient;
    private final AdminService adminService;
    private final TlqcnProperties tlqcnProperties;

    private final String successInfo = "测试完毕，请查看logs/function_test.log中[%s到%s]之间的日志，"
            + "发送的消息和消息的轨迹(如果开启了消息轨迹)可以在控制台查看";
    private final String errorInfo = "测试出现异常，请查看logs/application.log排查问题";
    private final String topicCreateFailed = "主题创建失败，请查看logs/application.log排查问题";
    private final String serviceUrl;
    private final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Autowired
    public FunctionTestServiceImpl(TlqcnClient tlqcnClient, AdminService adminService,
                                   TlqcnProperties tlqcnProperties) {
        this.tlqcnClient = tlqcnClient;
        this.adminService = adminService;
        this.tlqcnProperties = tlqcnProperties;
        this.serviceUrl = tlqcnProperties.getClient().getServiceUrl();
    }

    private <T> Producer<T> createProducer(String topic, Schema<T> schema) throws TlqcnClientException {
        ProducerBuilder<T> builder = tlqcnClient.newProducer(schema)
                // 必要参数。消息发送的目标主题。
                .topic(topic)
                .sendTimeout(1, TimeUnit.SECONDS);
        return builder.create();
    }

    @Override
    public String syncSendTest(String topic, int msgNum) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "--------同步发送消息测试开始----------");
        if (msgNum > 1000) {
            msgNum = 1000;
            log.info(FUNCTION_TEST, "测试消息数量超过1000条，自动设置为1000");
        }
        log.info(FUNCTION_TEST, "测试主题[{}],消息条数[{}]", topic, msgNum);
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            producer = createProducer(topic, Schema.INT64);
            startTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "即将按顺序同步发送[0, {}]的消息，前一条消息发送完毕才会进行下一条消息的发送", msgNum);
            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "同步发送消息，内容<{}>", i);
                long sendStartTime = System.currentTimeMillis();
                MessageId send = producer.send(i);
                long sendFinishTime = System.currentTimeMillis();
                log.info(FUNCTION_TEST, "同步发送成功，内容<{}>, 消息id<{}>, 发布延时<{}ms>", i, send, (sendFinishTime - sendStartTime));
            }
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "--------同步发送消息测试完毕----------");
        } catch (TlqcnClientException e) {
            log.error("同步发送测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String asyncSendTest(String topic, int msgNum) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "--------异步发送消息测试开始----------");
        if (msgNum > 1000) {
            msgNum = 1000;
            log.info(FUNCTION_TEST, "测试消息数量超过1000条，自动设置为1000");
        }
        log.info(FUNCTION_TEST, "测试主题[{}],消息条数[{}]", topic, msgNum);
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            producer = createProducer(topic, Schema.INT64);
            startTime = LocalDateTime.now();
            CountDownLatch countDownLatch = new CountDownLatch(msgNum);
            log.info(FUNCTION_TEST, "即将按顺序异步发送[0, {}]的消息", msgNum);
            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "异步发送消息，内容<{}>", i);
                long finalI = i;
                long sendStartTime = System.currentTimeMillis();
                producer.sendAsync(i)
                        .thenAccept(send -> {
                            long sendFinishTime = System.currentTimeMillis();
                            log.info(FUNCTION_TEST, "异步发送成功，内容<{}>, 消息id<{}>, 发布延时<{}ms>", finalI, send, (sendFinishTime - sendStartTime));
                            countDownLatch.countDown();
                        })
                        .exceptionally(throwable -> {
                            log.error("发送异常", throwable);
                            countDownLatch.countDown();
                            return null;
                        });
            }
            countDownLatch.await();
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "--------异步发送消息测试完毕----------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("异步发送测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String subscribeTypeTest(String topic, int msgNum, int consumerNum, SubscriptionType subscriptionType) {
        adminService.clearAndCreateTopic(topic);
        LocalDateTime startTime = LocalDateTime.now();
        subscribeTest(topic, subscriptionType, consumerNum, msgNum);
        LocalDateTime endTime = LocalDateTime.now();
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    /**
     * 订阅类型测试
     */
    private void subscribeTest(String topic, SubscriptionType subscriptionType, int consumerNum, int msgNum) {
        try {
            log.info(FUNCTION_TEST, "+++++++++++{}订阅类型测试开始+++++++++++", subscriptionType);
            if (consumerNum > 5) {
                consumerNum = 5;
                log.info(FUNCTION_TEST, "测试消费者数量超过5个，自动设置为5个");
            }
            if (msgNum > 1000) {
                msgNum = 1000;
                log.info(FUNCTION_TEST, "测试消息数量超过1000条，自动设置为1000");
            }
            if (msgNum < 10) {
                msgNum = 10;
                log.info(FUNCTION_TEST, "测试消息条数低于10条，自动设置为10条");
            }
            log.info(FUNCTION_TEST, "测试主题[{}],消息条数[{}],消费者数量[{}]", topic, msgNum, consumerNum);
            List<Consumer<Long>> consumers = new ArrayList<>();
            for (int i = 0; i < consumerNum; i++) {
                try {
                    Consumer<Long> consumer = createConsumer(topic, null, "sub_type_test",
                            "consumer_" + i, subscriptionType, Schema.INT64);
                    consumers.add(consumer);
                    log.info(FUNCTION_TEST, "创建消费者<{}>成功", "consumer_" + i);
                } catch (TlqcnClientException e) {
                    log.error(FUNCTION_TEST, "创建消费者<{}>异常", "consumer_" + i, e);
                }
            }
            Producer<Long> producer = createProducer(topic, Schema.INT64);
            for (long i = 0; i < msgNum; i++) {
                if (subscriptionType == SubscriptionType.Failover && i == 5) {
                    Consumer<Long> remove = consumers.remove(0);
                    remove.close();
                    log.info(FUNCTION_TEST, "消费者<{}>关闭成功", remove.getConsumerName());
                }
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send;
                if (subscriptionType == SubscriptionType.Key_Shared) {
                    send = producer.newMessage()
                            .key(String.valueOf(i))
                            .value(i)
                            .send();
                } else {
                    send = producer.send(i);
                }
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }

            Thread.sleep(1000);
            consumers.forEach(consumer -> {
                try {
                    consumer.close();
                    log.info("消费者<{}>关闭成功", consumer.getConsumerName());
                } catch (TlqcnClientException e) {
                    log.error("消费者关闭异常", e);
                }
            });

            log.info(FUNCTION_TEST, "+++++++++++{}订阅类型测试完毕+++++++++++", subscriptionType);
            producer.close();
            log.info("生产者关闭成功");
        } catch (InterruptedException | TlqcnClientException e) {
            log.error("订阅类型测试异常", e);
        }
    }

    @Override
    public String messageFilterTest(String topic) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        Consumer<String> tagFilter = null;
        Consumer<String> sql92Filter = null;
        Producer<String> producer = null;
        log.info(FUNCTION_TEST, "--------消息过滤测试开始----------");
        log.info(FUNCTION_TEST, "测试主题[{}]", topic);
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            startTime = LocalDateTime.now();
            String tag1 = "tag1";
            String tag2 = "tag2";
            Map<String, String> tagFilterProperties = new HashMap<>();
            tagFilterProperties.put(tag1, "123");
            tagFilterProperties.put(tag2, "321");
            tagFilter = filterSubscribe(topic, "tag_filter", tagFilterProperties);
            Map<String, String> sql92FilterProperties = new HashMap<>();
            sql92FilterProperties.put("TLQ_CN_SQL92_FILTER_EXPRESSION", "tag1 IS NOT NULL AND (tag1 IN ('123', '345'))");
            sql92Filter = filterSubscribe(topic, "sql92_filter", sql92FilterProperties);

            producer = createProducer(topic, Schema.STRING);
            List<String> list = Lists.newArrayList("123", "234", "345", "543", "321");
            for (int i = 0; i < 10; i++) {
                int first = i % 5;
                int next = (i + 1) % 5;
                String message = MessageFormat.format("tag1<{0}> : tag2 <{1}>", list.get(first), list.get(next));
                producer.newMessage()
                        .value(message)
                        .property(tag1, list.get(first))
                        .property(tag2, list.get(next))
                        .send();
                log.info(FUNCTION_TEST, "消息发送成功：{}", message);
            }

            Thread.sleep(3000);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "--------消息过滤测试完毕----------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("消息过滤测试异常", e);
            return errorInfo;
        } finally {
            try {
                if (tagFilter != null) {
                    tagFilter.close();
                }
                if (sql92Filter != null) {
                    sql92Filter.close();
                }
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    private Consumer<String> filterSubscribe(String topic, String subName, Map<String, String> subscriptionProperties)
            throws TlqcnClientException {
        log.info(FUNCTION_TEST, "{} 订阅成功，过滤参数 {}", subName, subscriptionProperties);
        return tlqcnClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionProperties(subscriptionProperties)
                .messageListener((MessageListener<String>) (consumer, msg) -> {
                    log.info(FUNCTION_TEST, "订阅名称<{}>消费消息<{}>", subName, msg.getValue());
                    try {
                        consumer.acknowledge(msg);
                    } catch (TlqcnClientException e) {
                        log.error("消息确认异常", e);
                    }

                })
                // 订阅
                .subscribe();
    }

    @Override
    public String messageSeekTest(String topic) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        Consumer<Long> consumer = null;
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "---------------消息回溯测试开始------------------");
        log.info(FUNCTION_TEST, "测试主题[{}]", topic);
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            consumer = createConsumer(topic, null, "seek_sub",
                    "seek_consumer", SubscriptionType.Shared, Schema.INT64);
            startTime = LocalDateTime.now();
            producer = createProducer(topic, Schema.INT64);
            MessageId messageId = null;
            for (long i = 0; i < 10; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                if (i == 5) {
                    messageId = send;
                }
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }

            Thread.sleep(3000);
            adminService.getTlqcnAdmin().topics().resetCursor(topic, "seek_sub", messageId);
            log.info(FUNCTION_TEST, "重置游标成功，游标位置<{}>", messageId);
            Thread.sleep(3000);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------消息回溯测试完毕-------------");
        } catch (TlqcnClientException | InterruptedException | TlqcnAdminException e) {
            log.error("消息回溯测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                    log.info("消费者<{}>关闭成功", consumer.getConsumerName());
                }
                if (producer != null) {
                    producer.close();
                    log.info("生产者关闭成功");
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String broadcastConsumeTest(String topic) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "-----------广播消费测试开始------------");
        log.info(FUNCTION_TEST, "测试主题[{}]", topic);
        List<Reader<Long>> readers = new ArrayList<>();
        Producer<Long> producer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            startTime = LocalDateTime.now();
            for (int i = 0; i < 3; i++) {
                try {
                    int finalI = i;
                    Reader<Long> longReader = tlqcnClient.newReader(Schema.INT64)
                            .topic(topic)
                            .readerName("reader_" + finalI)
                            .startMessageId(MessageId.latest)
                            .readerListener((reader, msg) -> {
                                log.info(FUNCTION_TEST, "reader<{}>消费消息<{}>", "reader_" + finalI, msg.getValue());
                            })
                            .create();
                    readers.add(longReader);
                    log.info(FUNCTION_TEST, "创建reader<{}>成功", "reader_" + i);
                } catch (TlqcnClientException e) {
                    log.error("创建reader<{}>异常", "reader_" + i, e);
                }
            }
            producer = createProducer(topic, Schema.INT64);
            for (long i = 0; i < 10; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }

            Thread.sleep(3000);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------广播消费测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("广播消费测试出现异常", e);
            return errorInfo;
        } finally {
            readers.forEach(reader -> {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("reader关闭异常", e);
                }
            });
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String deadLetterQueueTest(String topic) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "---------------死信队列测试开始---------------");
        log.info(FUNCTION_TEST, "测试主题[{}]", topic);
        String deadLetterTopic = topic + "_DLQ";
        if (!adminService.clearAndCreateTopic(deadLetterTopic)) {
            return topicCreateFailed;
        }
        Consumer<String> consumer1 = null;
        Consumer<String> consumer2 = null;
        Producer<String> producer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            startTime = LocalDateTime.now();
            consumer1 = createConsumer(topic, deadLetterTopic, topic + "_sub",
                    topic + "_consumer", SubscriptionType.Shared, Schema.STRING);

            consumer2 = createConsumer(deadLetterTopic, null, deadLetterTopic + "_sub",
                    deadLetterTopic + "_consumer", SubscriptionType.Shared, Schema.STRING);

            producer = createProducer(topic, Schema.STRING);
            for (int i = 0; i < 5; i++) {
                String message = "消息测试-" + i;
                MessageId send = producer.send(message);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", message, send);
            }
            producer.flush();

            Thread.sleep(1000 * 60 * 1);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------死信队列测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("死信队列测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (consumer1 != null) {
                    consumer1.close();
                }
                if (consumer2 != null) {
                    consumer2.close();
                }
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    private <T> Consumer<T> createConsumer(String topic, String deadLetterTopic,
                                           String subName, String consumerName,
                                           SubscriptionType subscriptionType, Schema<T> schema) throws TlqcnClientException {
        AtomicInteger count = new AtomicInteger(0);
        ConsumerBuilder<T> consumerBuilder = tlqcnClient.newConsumer(schema)
                .topic(topic)
                .consumerName(consumerName)
                .subscriptionName(subName)
                .ackTimeout(3, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(5, TimeUnit.SECONDS)
                .subscriptionType(subscriptionType)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener((MessageListener<T>) (consumer, msg) -> {
                    if (deadLetterTopic == null) {
                        log.info(FUNCTION_TEST, "消费者<{}>消费消息<{}>", consumer.getConsumerName(), msg.getValue());
                    } else {
                        log.info(FUNCTION_TEST, "消费者<{}>消费消息<{}>重试次数<{}>", consumer.getConsumerName(), msg.getValue(),
                                msg.getRedeliveryCount());
                    }
                    if (deadLetterTopic == null) {
                        try {
                            consumer.acknowledge(msg);
                        } catch (TlqcnClientException e) {
                            log.error("签收消息失败", e);
                        }
                    } else {
                        consumer.negativeAcknowledge(msg);
                        log.info(FUNCTION_TEST, "消费者<{}>不签收消息<{}>", consumer.getConsumerName(), msg.getValue());
                    }
                });
        if (deadLetterTopic != null) {
            consumerBuilder.deadLetterPolicy(DeadLetterPolicy.builder()
                    .maxRedeliverCount(2)
                    .deadLetterTopic(deadLetterTopic)
                    .build());
        }
        return consumerBuilder.subscribe();
    }

    @Override
    public String consumerRetryTest(String topic) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "---------------消费者重试测试开始---------------");
        LocalDateTime startTime = LocalDateTime.now();
        log.info(FUNCTION_TEST, "请使用死信队列测试，死信队列包含了消息重试");
        LocalDateTime endTime = LocalDateTime.now();
        log.info(FUNCTION_TEST, "---------------消费者重试测试完毕---------------");
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String delayMessageTest(String topic, long delaySeconds) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "---------------延时消息测试开始---------------");
        log.info(FUNCTION_TEST, "测试主题[{}],延时时间[{}]秒", topic, delaySeconds);
        Consumer<String> consumer = null;
        Producer<String> producer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            consumer = createConsumer(topic, null, "delay_sub",
                    "delay_sub_consumer", SubscriptionType.Shared, Schema.STRING);

            producer = createProducer(topic, Schema.STRING);
            startTime = LocalDateTime.now();
            MessageId send1 = producer.newMessage()
                    .deliverAfter(delaySeconds, TimeUnit.SECONDS)
                    .value("延时消息!")
                    .send();
            log.info(FUNCTION_TEST, "延时消息发送成功，延时<{}>秒,消息id<{}>", delaySeconds, send1);

            Thread.sleep(1000 * (delaySeconds + 10));
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------延时消息测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("延时消息测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
                if (consumer != null) {
                    consumer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String scheduledMessageTest(String topic, Long timestamp) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        if (timestamp == null) {
            timestamp = System.currentTimeMillis() + 30_1000;
        }
        ZoneId targetZone = ZoneId.of("Asia/Shanghai");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss zzz");
        String formattedTime = Instant.ofEpochMilli(timestamp)
                .atZone(targetZone) // 核心：指定时区
                .format(formatter);
        log.info(FUNCTION_TEST, "---------------定时消息测试开始---------------");
        log.info(FUNCTION_TEST, "测试主题[{}],定时时间戳[{}],时间[{}]", topic, timestamp, formattedTime);
        Consumer<String> consumer = null;
        Producer<String> producer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            consumer = createConsumer(topic, null, "delay_sub",
                    "delay_sub_consumer", SubscriptionType.Shared, Schema.STRING);

            producer = createProducer(topic, Schema.STRING);
            startTime = LocalDateTime.now();
            MessageId send2 = producer.newMessage()
                    .deliverAt(timestamp)
                    .value("定时消息!")
                    .send();
            log.info(FUNCTION_TEST, "发送成功，指定消费时间<{}>，消息id<{}>", timestamp, send2);

            if (timestamp > System.currentTimeMillis()) {
                Thread.sleep(timestamp - System.currentTimeMillis() + 2000);
            } else {
                Thread.sleep(2000);
            }
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------定时消息测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("定时消息测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
                if (consumer != null) {
                    consumer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String messageOrderTest(String topic, int msgNum) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "---------------消息有序性测试开始---------------");
        log.info(FUNCTION_TEST, "测试主题[{}],消息条数[{}]", topic, msgNum);
        Producer<Long> producer = null;
        Consumer<Long> consumer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            producer = createProducer(topic, Schema.INT64);
            startTime = LocalDateTime.now();
            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }
            log.info(FUNCTION_TEST, "++++++++++++消息发送完毕++++++++++++");
            log.info(FUNCTION_TEST, "++++++++++++开始消费消息++++++++++++");
            consumer = createConsumer(topic, null, "order_sub",
                    "order_consumer", SubscriptionType.Exclusive, Schema.INT64);

            Thread.sleep(1000 * 3);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------消息有序性测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("消息有序性测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
                if (consumer != null) {
                    consumer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String gmMessageTest(String topic, String privateKeyPath, String publicKeyPath) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "---------------国密消息测试开始---------------");
        log.info(FUNCTION_TEST, "测试主题[{}]", topic);
        Consumer<String> consumer = null;
        Producer<String> producer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            startTime = LocalDateTime.now();
            consumer = tlqcnClient.newConsumer(Schema.STRING)
                    .topic(topic)
                    .subscriptionName("gmTest")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageCrypto(new MessageCryptoGm4Consumer())
                    .cryptoKeyReader(new RawFileKeyReader(publicKeyPath, privateKeyPath))
                    .subscribe();

            producer = tlqcnClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .addEncryptionKey("key1")
                    .messageCrypto(new MessageCryptoGm4Producer())
                    .cryptoKeyReader(new RawFileKeyReader(publicKeyPath, privateKeyPath))
                    .create();
            producer.newMessage()
                    .value("国密消息!")
                    .send();
            log.info(FUNCTION_TEST, "国密消息发送成功");

            Message<String> receive = consumer.receive();
            consumer.acknowledge(receive);
            log.info(FUNCTION_TEST, "消费者消费到消息:<{}>", receive.getValue());
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------国密消息测试完毕---------------");
        } catch (TlqcnClientException e) {
            log.error("国密消息测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                }
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String gmTlsTest(String topic) {
        if (!adminService.clearAndCreateTopic(topic)) {
            return topicCreateFailed;
        }
        log.info(FUNCTION_TEST, "---------------国密通信测试开始---------------");
        LocalDateTime startTime = LocalDateTime.now();
        log.info(FUNCTION_TEST, "国密TLS测试暂时无法通过日志观察");
        LocalDateTime endTime = LocalDateTime.now();
        log.info(FUNCTION_TEST, "---------------国密通信测试完毕---------------");
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String messagePriorityTest(String topic, int totalPriority, boolean isAbsolutePriority) {
        log.info(FUNCTION_TEST, "---------------消息优先级测试开始---------------");
        if (totalPriority > 10) {
            totalPriority = 10;
            log.info(FUNCTION_TEST, "优先级超过10级，自动设置为10级");
        }
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            startTime = LocalDateTime.now();
            for (int i = 0; i < totalPriority; i++) {
                adminService.clearAndCreateTopic(topic + i);
                log.info(FUNCTION_TEST, "测试主题[{}]", topic + i);
            }
            if (isAbsolutePriority) {
                log.info(FUNCTION_TEST, "测试消息的绝对优先级");
            } else {
                log.info(FUNCTION_TEST, "测试消息的相对优先级");
            }
            PriorityUtil.startTest(totalPriority, topic, serviceUrl, isAbsolutePriority);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------消息优先级测试完毕---------------");
        } catch (ExecutionException | InterruptedException | TlqcnClientException e) {
            log.error("消息优先级测试出现异常", e);
            return errorInfo;
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String failoverTest(String topic, int msgNum) {
        log.info(FUNCTION_TEST, "---------------故障转移测试开始---------------");
        log.info(FUNCTION_TEST, "测试主题[{}],消息条数[{}]", topic, msgNum);
        TlqcnClient client = null;
        TlqcnAdmin primaryAdmin = null;
        TlqcnAdmin secondaryAdmin = null;
        Consumer<Long> failoverTestConsumer = null;
        Producer<Long> producer = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            ServiceUrlProvider failover = AutoClusterFailover.builder()
                    .primary(tlqcnProperties.getClient().getFailoverConfig().getPrimaryUrl())
                    .secondary(List.of(tlqcnProperties.getClient().getFailoverConfig().getSecondaryUrl()))
                    .failoverDelay(10, TimeUnit.SECONDS)
                    .switchBackDelay(30, TimeUnit.SECONDS)
                    .checkInterval(1000, TimeUnit.MILLISECONDS)
                    .build();

            client = TlqcnClient.builder()
                    .serviceUrlProvider(failover)
                    .build();

            failover.initialize(client);

            primaryAdmin = TlqcnAdmin.builder()
                    .serviceHttpUrl(tlqcnProperties.getClient().getFailoverConfig().getPrimaryHttpUrl())
                    .build();

            secondaryAdmin = TlqcnAdmin.builder()
                    .serviceHttpUrl(tlqcnProperties.getClient().getFailoverConfig().getSecondaryHttpUrl())
                    .build();

            try {
                primaryAdmin.topics().delete(topic, true);
                log.info("主集群删除topic<{}>成功", topic);
                secondaryAdmin.topics().delete(topic, true);
                log.info("备集群删除topic<{}>成功", topic);
            } catch (Exception e) {
                log.info("删除topic<{}>失败", topic);
            }

            try {
                primaryAdmin.topics().createNonPartitionedTopic(topic);
                log.info("主集群创建topic<{}>成功", topic);
                secondaryAdmin.topics().createNonPartitionedTopic(topic);
                log.info("备集群创建topic<{}>成功", topic);
            } catch (Exception e) {
                log.info("创建topic<{}>失败", topic);
                return topicCreateFailed;
            }

            startTime = LocalDateTime.now();
            failoverTestConsumer = client.newConsumer(Schema.INT64)
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("failoverSub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener((MessageListener<Long>) (consumer, msg) -> {
                        log.info(FUNCTION_TEST, "消费者<{}>消费消息<{}>，发送者<{}>,复制集群<{}>", consumer.getConsumerName(), msg.getValue(), msg.getProducerName(), msg.getReplicatedFrom());
                    })
                    .replicateSubscriptionState(true)
                    .ackTimeout(0, TimeUnit.SECONDS)
                    .subscribe();

            producer = client.newProducer(Schema.INT64)
                    .topic(topic)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .create();

            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }

            Thread.sleep(1000 * 13);
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "---------------故障转移测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("故障转移测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
                if (failoverTestConsumer != null) {
                    failoverTestConsumer.close();
                }
                if (client != null) {
                    client.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }

    @Override
    public String transactionTest(String inputTopic, String outputTopicOne, String outputTopicTwo, int msgNum) {
        log.info(FUNCTION_TEST, "---------------事务测试开始---------------");
        if (msgNum > 5) {
            msgNum = 5;
            log.info(FUNCTION_TEST, "事务测试消息数量大于5条，自动设置为5条");
        }
        log.info(FUNCTION_TEST, "测试主题inputTopic[{}]", inputTopic);
        log.info(FUNCTION_TEST, "测试主题outputTopicOne[{}]", outputTopicOne);
        log.info(FUNCTION_TEST, "测试主题outputTopicTwo[{}]", outputTopicTwo);
        if (!adminService.clearAndCreateTopic(inputTopic)) {
            return topicCreateFailed;
        }
        if (!adminService.clearAndCreateTopic(outputTopicOne)) {
            return topicCreateFailed;
        }
        if (!adminService.clearAndCreateTopic(outputTopicTwo)) {
            return topicCreateFailed;
        }
        TlqcnClient client = null;
        Producer<String> inputProducer = null;
        Producer<String> outputProducerOne = null;
        Producer<String> outputProducerTwo = null;
        Consumer<String> inputConsumer = null;
        Consumer<String> outputConsumerOne = null;
        Consumer<String> outputConsumerTwo = null;
        LocalDateTime startTime = null;
        LocalDateTime endTime = null;
        try {
            client = TlqcnClient.builder()
                    .serviceUrl(tlqcnProperties.getClient().getServiceUrl())
                    .enableTransaction(true)
                    .build();
            startTime = LocalDateTime.now();
            // create three producers to produce messages to input and output topics.
            ProducerBuilder<String> producerBuilder = client.newProducer(Schema.STRING);
            inputProducer = producerBuilder.topic(inputTopic)
                    .sendTimeout(0, TimeUnit.SECONDS).create();
            outputProducerOne = producerBuilder.topic(outputTopicOne)
                    .sendTimeout(0, TimeUnit.SECONDS).create();
            outputProducerTwo = producerBuilder.topic(outputTopicTwo)
                    .sendTimeout(0, TimeUnit.SECONDS).create();
            // create three consumers to consume messages from input and output topics.
            inputConsumer = client.newConsumer(Schema.STRING)
                    .subscriptionName("sub").topic(inputTopic).subscribe();
            outputConsumerOne = client.newConsumer(Schema.STRING)
                    .subscriptionName("sub").topic(outputTopicOne).subscribe();
            outputConsumerTwo = client.newConsumer(Schema.STRING)
                    .subscriptionName("sub").topic(outputTopicTwo).subscribe();

            // produce messages to input topics.
            for (int i = 0; i < msgNum; i++) {
                String msg = "Hello TongLINK/Q-CN! count : " + i;
                MessageId send = inputProducer.send(msg);
                log.info(FUNCTION_TEST, "向主题<{}>发送消息<{}>,消息id<{}>", inputTopic, msg, send);
            }

            // consume messages and produce them to output topics with transactions.
            for (int i = 0; i < msgNum; i++) {

                // the consumer successfully receives messages.
                Message<String> message = inputConsumer.receive();
                log.info(FUNCTION_TEST, "主题<{}>收取消息<{}>,消息id<{}>", inputTopic, message.getValue(), message.getMessageId());

                // create transactions.
                // The transaction timeout is specified as 10 seconds.
                // If the transaction is not committed within 10 seconds, the transaction is automatically aborted.
                Transaction txn = null;
                try {
                    txn = client.newTransaction()
                            .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
                    log.info(FUNCTION_TEST, "开启一个事务<{}>", txn.getTxnID());
                    // you can process the received message with your use case and business logic.

                    // the producers produce messages to output topics with transactions
                    String outputTopicOneMsg = "Hello TongLINK/Q-CN! outputTopicOne count : " + i;
                    String outputTopicTwoMsg = "Hello TongLINK/Q-CN! outputTopicTwo count : " + i;
                    MessageId sendOne = outputProducerOne.newMessage(txn).value(outputTopicOneMsg).send();
                    log.info(FUNCTION_TEST, "向主题<{}>发送消息<{}>,带事务,消息id<{}>", outputTopicOne, outputTopicOneMsg, sendOne);
                    MessageId sendTwo = outputProducerTwo.newMessage(txn).value(outputTopicTwoMsg).send();
                    log.info(FUNCTION_TEST, "向主题<{}>发送消息<{}>,带事务,消息id<{}>", outputTopicTwo, outputTopicTwoMsg, sendTwo);

                    // the consumers acknowledge the input message with the transactions *individually*.
                    inputConsumer.acknowledgeAsync(message.getMessageId(), txn).get();
                    log.info(FUNCTION_TEST, "向主题<{}>确认消息<{}>,携带事务,消息id<{}>", inputTopic, message.getValue(), message.getMessageId());
                    // commit transactions.
                    txn.commit().get();
                    log.info(FUNCTION_TEST, "提交事务<{}>", txn.getTxnID());
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TlqcnClientException.TransactionConflictException)) {
                        // If TransactionConflictException is not thrown,
                        // you need to redeliver or negativeAcknowledge this message,
                        // or else this message will not be received again.
                        inputConsumer.negativeAcknowledge(message);
                    }

                    // If a new transaction is created,
                    // then the old transaction should be aborted.
                    if (txn != null) {
                        txn.abort();
                    }
                }
            }

            // Final result: consume messages from output topics and print them.
            for (int i = 0; i < msgNum; i++) {
                Message<String> message = outputConsumerOne.receive();
                log.info(FUNCTION_TEST, "主题<{}>收取消息<{}>,消息id<{}>", outputTopicOne, message.getValue(), message.getMessageId());
                outputConsumerOne.acknowledge(message);
            }

            for (int i = 0; i < msgNum; i++) {
                Message<String> message = outputConsumerTwo.receive();
                log.info(FUNCTION_TEST, "主题<{}>收取消息<{}>,消息id<{}>", outputTopicTwo, message.getValue(), message.getMessageId());
                outputConsumerTwo.acknowledge(message);
            }
            endTime = LocalDateTime.now();
            log.info(FUNCTION_TEST, "--------------事务测试完毕---------------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("事务测试出现异常", e);
            return errorInfo;
        } finally {
            try {
                if (inputProducer != null) {
                    inputProducer.close();
                }
                if (outputProducerOne != null) {
                    outputProducerOne.close();
                }
                if (outputProducerTwo != null) {
                    outputProducerTwo.close();
                }
                if (inputConsumer != null) {
                    inputConsumer.close();
                }
                if (outputConsumerOne != null) {
                    outputConsumerOne.close();
                }
                if (outputConsumerTwo != null) {
                    outputConsumerTwo.close();
                }
                if (client != null) {
                    client.close();
                }
            } catch (TlqcnClientException e) {
                log.error("资源关闭异常", e);
            }
        }
        return String.format(successInfo, FORMATTER.format(startTime), FORMATTER.format(endTime));
    }
}
