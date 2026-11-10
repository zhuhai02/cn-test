package com.tongtech.cntest.service;

import com.tongtech.tlqcn.client.MessageCryptoGm4Consumer;
import com.tongtech.tlqcn.client.MessageCryptoGm4Producer;
import com.tongtech.tlqcn.client.admin.TlqcnAdmin;
import com.tongtech.tlqcn.client.admin.TlqcnAdminException;
import com.tongtech.tlqcn.client.api.*;
import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.FunctionTestService;
import com.tongtech.cntest.utils.PriorityUtil;
import com.tongtech.cntest.utils.RawFileKeyReader;
import com.tongtech.tlqcn.client.api.transaction.Transaction;
import com.tongtech.tlqcn.client.impl.AutoClusterFailover;
import com.tongtech.tlqcn.shade.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class FunctionTestServiceImpl implements FunctionTestService {
    private static final Logger log = LoggerFactory.getLogger("FUNCTION_TEST_LOGGER");
    private static final Marker FUNCTION_TEST = MarkerFactory.getMarker("FUNCTION_TEST");

    private final TlqcnClient tlqcnClient;
    private final TlqcnAdmin tlqcnAdmin;
    private final TlqcnProperties tlqcnProperties;

    private final String topic;
    private final String publicKeyPath;
    private final String privateKeyPath;
    private final String serviceUrl;
    private final int consumerNum;
    private final int topicPartitionNum;
    private final int msgNum;

    @Autowired
    public FunctionTestServiceImpl(TlqcnClient tlqcnClient, TlqcnAdmin tlqcnAdmin,
                                   TlqcnProperties tlqcnProperties) {
        this.tlqcnClient = tlqcnClient;
        this.tlqcnAdmin = tlqcnAdmin;
        this.tlqcnProperties = tlqcnProperties;
        this.topic = tlqcnProperties.getFunctionTestConfig().getTopic();
        this.publicKeyPath = tlqcnProperties.getFunctionTestConfig().getPublicKeyPath();
        this.privateKeyPath = tlqcnProperties.getFunctionTestConfig().getPrivateKeyPath();
        this.serviceUrl = tlqcnProperties.getClient().getServiceUrl();
        this.consumerNum = tlqcnProperties.getFunctionTestConfig().getConsumerNum();
        this.topicPartitionNum = tlqcnProperties.getFunctionTestConfig().getTopicPartitionNum();
        this.msgNum = tlqcnProperties.getFunctionTestConfig().getMsgNum();
    }

    /**
     * 测试前先清理topic
     *
     * @param topic
     */
    private void clearAndCreateTopic(String topic) {
        try {
            tlqcnAdmin.topics().delete(topic, true);
            log.info("删除topic<{}>成功", topic);
        } catch (TlqcnAdminException e) {
        }

        try {
            tlqcnAdmin.topics().createNonPartitionedTopic(topic);
            log.info("创建topic<{}>成功", topic);
        } catch (TlqcnAdminException e) {
            log.error("创建topic<{}>异常", topic, e);
        }
    }

    private <T> Producer<T> createProducer(String topic, Schema<T> schema) throws TlqcnClientException {
        ProducerBuilder<T> builder = tlqcnClient.newProducer(schema)
                // 必要参数。消息发送的目标主题。
                .topic(topic)
                .sendTimeout(1, TimeUnit.SECONDS);
        return builder.create();
    }

    @Override
    public void startTest() {
        if (tlqcnProperties.getFunctionTestConfig().isEnabledSyncSendTest()) {
            syncSendTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledAsyncSendTest()) {
            asyncSendTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledSubscribeTypeTest()) {
            subscribeTypeTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledMessageFilterTest()) {
            messageFilterTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledMessageSeekTest()) {
            messageSeekTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledBroadcastConsumeTest()) {
            broadcastConsumeTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledMessageTtlTest()) {
            messageTtlTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledDeadLetterQueueTest()) {
            deadLetterQueueTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledConsumerRetryTest()) {
            consumerRetryTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledDelayMessageTest()) {
            delayMessageTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledScheduledMessageTest()) {
            scheduledMessageTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledMessageOrderTest()) {
            messageOrderTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledGmMessageTest()) {
            gmMessageTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledGmTlsTest()) {
            gmTlsTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledMessagePriorityTest()) {
            messagePriorityTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledFailoverTest()) {
            failoverTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledTransactionTest()) {
            transactionTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledBatchSendTest()) {
            batchSendTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledManualAckTest()) {
            manualAckTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledAutoDeleteSubTest()) {
            autoDeleteSubTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledMaxMessageSizeRejectTest()) {
            maxMessageSizeRejectTest();
        }
        if (tlqcnProperties.getFunctionTestConfig().isEnabledBackpressureTest()) {
            backpressureTest();
        }
    }

    @Override
    public void syncSendTest() {
        clearAndCreateTopic(topic);
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "--------同步发送消息测试开始----------");
        try {
            List<Consumer<Long>> consumers = new ArrayList<>();
            for (int i = 0; i < consumerNum; i++) {
                try {
                    Consumer<Long> consumer = createConsumer(topic, null, "Sync_sub",
                            "sync_consumer_" + i, SubscriptionType.Shared, Schema.INT64);
                    consumers.add(consumer);
                    log.info(FUNCTION_TEST, "创建消费者<{}>成功", "sync_consumer_" + i);
                } catch (TlqcnClientException e) {
                    log.error(FUNCTION_TEST, "创建消费者<{}>异常", "sync_consumer_" + i, e);
                }
            }

            Thread.sleep(3000);
            producer = createProducer(topic, Schema.INT64);
            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "同步发送消息，内容<{}>", i);
                long sendStartTime = System.currentTimeMillis();
                MessageId send = producer.send(i);
                long sendFinishTime = System.currentTimeMillis();
                log.info(FUNCTION_TEST, "同步发送成功，内容<{}>, 消息id<{}>, time<{}ms>", i, send, (sendFinishTime - sendStartTime));
            }

            Thread.sleep(1000 * 3);
            consumers.forEach(consumer -> {
                try {
                    consumer.close();
                    log.info("消费者<{}>关闭成功", consumer.getConsumerName());
                } catch (TlqcnClientException e) {
                    log.error("消费者关闭异常", e);
                }
            });
            producer.close();
            log.info("生产者关闭成功");
            log.info(FUNCTION_TEST, "--------同步发送消息测试完毕----------");
        } catch (TlqcnClientException e) {
            log.error("发送异常", e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
    }

    @Override
    public void asyncSendTest() {
        clearAndCreateTopic(topic);
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "--------异步发送消息测试开始----------");
        try {
            List<Consumer<Long>> consumers = new ArrayList<>();
            for (int i = 0; i < consumerNum; i++) {
                try {
                    Consumer<Long> consumer = createConsumer(topic, null, "Aync_sub",
                            "aync_consumer_" + i, SubscriptionType.Shared, Schema.INT64);
                    consumers.add(consumer);
                    log.info(FUNCTION_TEST, "创建消费者<{}>成功", "aync_consumer_" + i);
                } catch (TlqcnClientException e) {
                    log.error(FUNCTION_TEST, "创建消费者<{}>异常", "aync_consumer_" + i, e);
                }
            }
            Thread.sleep(3000);

            producer = createProducer(topic, Schema.INT64);
            CountDownLatch countDownLatch = new CountDownLatch(10);
            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "异步发送消息，内容<{}>", i);
                long finalI = i;
                long sendStartTime = System.currentTimeMillis();
                producer.sendAsync(i)
                        .thenAccept(send -> {
                            long sendFinishTime = System.currentTimeMillis();
                            log.info(FUNCTION_TEST, "异步发送成功，内容<{}>, 消息id<{}>, time<{}ms>", finalI, send, (sendFinishTime - sendStartTime));
                            countDownLatch.countDown();
                        })
                        .exceptionally(throwable -> {
                            log.error("发送异常", throwable);
                            countDownLatch.countDown();
                            return null;
                        });

            }
            countDownLatch.await();

            Thread.sleep(3000);
            consumers.forEach(consumer -> {
                try {
                    consumer.close();
                    log.info("消费者<{}>关闭成功", consumer.getConsumerName());
                } catch (TlqcnClientException e) {
                    log.error("消费者关闭异常", e);
                }
            });
            log.info(FUNCTION_TEST, "--------异步发送消息测试完毕----------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("发送异常", e);
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
    }

    @Override
    public void subscribeTypeTest() {
        log.info(FUNCTION_TEST, "--------订阅类型测试开始----------");
        subscribeTest("Exclusive_sub", SubscriptionType.Exclusive, consumerNum);
        subscribeTest("Failover_sub", SubscriptionType.Failover, consumerNum);
        subscribeTest("Shared_sub", SubscriptionType.Shared, consumerNum);
        subscribeTest("Key_Shared_sub", SubscriptionType.Key_Shared, consumerNum);
        log.info(FUNCTION_TEST, "--------订阅类型测试完毕----------");
    }

    /**
     * 独占订阅测试
     */
    private void subscribeTest(String subName, SubscriptionType subscriptionType, int consumerNum) {
        clearAndCreateTopic(topic);
        try {
            log.info(FUNCTION_TEST, "+++++++++++{}订阅类型测试开始+++++++++++", subscriptionType);
            List<Consumer<Long>> consumers = new ArrayList<>();
            for (int i = 0; i < consumerNum; i++) {
                try {
                    Consumer<Long> consumer = createConsumer(topic, null, subName,
                            "consumer_" + i, subscriptionType, Schema.INT64);
                    consumers.add(consumer);
                    log.info(FUNCTION_TEST, "创建消费者<{}>成功", "consumer_" + i);
                } catch (TlqcnClientException e) {
                    log.error(FUNCTION_TEST, "创建消费者<{}>异常", "consumer_" + i, e);
                }
            }
            Thread.sleep(3000);
            Producer<Long> producer = createProducer(topic, Schema.INT64);
            for (long i = 0; i < msgNum; i++) {
                if (subscriptionType == SubscriptionType.Failover && i == 50) {
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

            Thread.sleep(3000);
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
    public void messageFilterTest() {
        clearAndCreateTopic(topic);
        try {
            log.info(FUNCTION_TEST, "--------消息过滤测试开始----------");
            String tag1 = "tag1";
            String tag2 = "tag2";
            Map<String, String> tagFilterProperties = new HashMap<>();
            tagFilterProperties.put(tag1, "123");
            tagFilterProperties.put(tag2, "321");
            Consumer<String> tagFilter = filterSubscribe("tag_filter", tagFilterProperties);
            Map<String, String> sql92FilterProperties = new HashMap<>();
            sql92FilterProperties.put("TLQ_CN_SQL92_FILTER_EXPRESSION", "tag1 IS NOT NULL AND (tag1 IN ('123', '345'))");
            Consumer<String> sql92Filter = filterSubscribe("sql92_filter", sql92FilterProperties);


            Producer<String> producer = createProducer(topic, Schema.STRING);
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
            log.info(FUNCTION_TEST, "--------消息过滤测试完毕----------");
            tagFilter.close();
            sql92Filter.close();
            producer.close();
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("消息过滤测试异常", e);
        }
    }

    private Consumer<String> filterSubscribe(String subName, Map<String, String> subscriptionProperties)
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
    public void messageSeekTest() {
        clearAndCreateTopic(topic);
        Consumer<Long> consumer = null;
        log.info(FUNCTION_TEST, "---------------消息回溯测试开始------------------");
        try {
            consumer = createConsumer(topic, null, "seek_sub",
                    "seek_consumer", SubscriptionType.Shared, Schema.INT64);
        } catch (TlqcnClientException e) {
            log.error("创建消费者异常", e);
        }
        Producer<Long> producer = null;
        MessageId messageId = null;
        try {
            producer = createProducer(topic, Schema.INT64);
            messageId = null;
            for (long i = 0; i < 10; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                if (i == 5) {
                    messageId = send;
                }
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }
        } catch (TlqcnClientException e) {
            log.error("发送异常", e);
        }

        try {
            Thread.sleep(3000);
            tlqcnAdmin.topics().resetCursor(topic, "seek_sub", messageId);
            log.info(FUNCTION_TEST, "重置游标成功，游标位置<{}>", messageId);
            Thread.sleep(3000);
        } catch (InterruptedException | TlqcnAdminException e) {
        }

        log.info(FUNCTION_TEST, "---------------消息回溯测试完毕-------------");
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
            log.error("消费者关闭异常", e);
        }

    }

    @Override
    public void broadcastConsumeTest() {
        clearAndCreateTopic(topic);
        log.info(FUNCTION_TEST, "-----------广播消费测试开始------------");
        List<Reader<Long>> readers = new ArrayList<>();
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
        Producer<Long> producer = null;
        try {
            producer = createProducer(topic, Schema.INT64);
            for (long i = 0; i < 10; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }
        } catch (TlqcnClientException e) {
            log.error("发送异常", e);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
        }
        log.info(FUNCTION_TEST, "---------------广播消费测试完毕---------------");
        readers.forEach(reader -> {
            try {
                reader.close();
            } catch (TlqcnClientException e) {
                log.error("reader关闭异常", e);
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

    @Override
    public void messageTtlTest() {

    }

    @Override
    public void deadLetterQueueTest() {
        clearAndCreateTopic(topic);
        log.info(FUNCTION_TEST, "---------------死信队列测试开始---------------");
        String deadLetterTopic = topic + "_DLQ";
        clearAndCreateTopic(deadLetterTopic);
        try {
            Consumer<String> consumer1 = createConsumer(topic, deadLetterTopic, topic + "_sub",
                    topic + "_consumer", SubscriptionType.Shared, Schema.STRING);

            Consumer<String> consumer2 = createConsumer(deadLetterTopic, null, deadLetterTopic + "_sub",
                    deadLetterTopic + "_consumer", SubscriptionType.Shared, Schema.STRING);

            Producer<String> producer = createProducer(topic, Schema.STRING);
            for (int i = 0; i < 5; i++) {
                String message = "消息测试-" + i;
//                producer.sendAsync(message);
                MessageId send = producer.send(message);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", message, send);
            }
            producer.flush();
            producer.close();

            Thread.sleep(1000 * 60 * 1);
            log.info(FUNCTION_TEST, "---------------死信队列测试完毕---------------");
            consumer1.close();
            consumer2.close();
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("测试出现异常", e);
        }
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
    public void consumerRetryTest() {

    }

    @Override
    public void delayMessageTest() {
        clearAndCreateTopic(topic);
        log.info(FUNCTION_TEST, "---------------延时消息测试开始---------------");
        try {
            Consumer<String> consumer = createConsumer(topic, null, "delay_sub",
                    "delay_sub_consumer", SubscriptionType.Shared, Schema.STRING);

            Producer<String> producer = createProducer(topic, Schema.STRING);
            long delay = 100;
            MessageId send1 = producer.newMessage()
                    .deliverAfter(delay, TimeUnit.SECONDS)
                    .value("延时消息!")
                    .send();
            log.info(FUNCTION_TEST, "延时消息发送成功，延时<{}>秒,消息id<{}>", delay, send1);

            Thread.sleep(1000 * 110);
            log.info(FUNCTION_TEST, "---------------延时消息测试完毕---------------");
            producer.close();
            consumer.close();
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("测试程序出现异常", e);
        }
    }

    @Override
    public void scheduledMessageTest() {
        clearAndCreateTopic(topic);
        log.info(FUNCTION_TEST, "---------------定时消息测试开始---------------");
        try {
            Consumer<String> consumer = createConsumer(topic, null, "delay_sub",
                    "delay_sub_consumer", SubscriptionType.Shared, Schema.STRING);

            Producer<String> producer = createProducer(topic, Schema.STRING);
            LocalDateTime localDateTime = LocalDateTime.now().plusSeconds(10);
            // 转换为ZonedDateTime，默认时区
            ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("Asia/Shanghai"));
            // 转换为Instant
            Instant instant = zonedDateTime.toInstant();
            // 获取时间戳（毫秒）
            long timestamp = instant.toEpochMilli();
            MessageId send2 = producer.newMessage()
                    .deliverAt(timestamp)
                    .value("定时消息!")
                    .send();
            log.info(FUNCTION_TEST, "发送成功，指定消费时间<{}>，消息id<{}>", localDateTime, send2);

            Thread.sleep(1000 * 13);
            log.info(FUNCTION_TEST, "---------------定时消息测试完毕---------------");
            producer.close();
            consumer.close();
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("测试程序出现异常", e);
        }
    }

    @Override
    public void messageOrderTest() {
        clearAndCreateTopic(topic);
        log.info(FUNCTION_TEST, "---------------消息有序性测试开始---------------");
        try {
            Producer<Long> producer = createProducer(topic, Schema.INT64);

            for (long i = 0; i < 10; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }
            log.info(FUNCTION_TEST, "++++++++++++消息发送完毕++++++++++++");
            log.info(FUNCTION_TEST, "++++++++++++开始消费消息++++++++++++");
            Consumer<Long> consumer = createConsumer(topic, null, "order_sub",
                    "order_consumer", SubscriptionType.Exclusive, Schema.INT64);

            Thread.sleep(1000 * 3);
            log.info(FUNCTION_TEST, "---------------消息有序性测试完毕---------------");
            producer.close();
            consumer.close();
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("测试程序出现异常", e);
        }
    }

    @Override
    public void gmMessageTest() {
        try {
            clearAndCreateTopic(topic);
            log.info(FUNCTION_TEST, "---------------国密消息测试开始---------------");
            Consumer<String> consumer = tlqcnClient.newConsumer(Schema.STRING)
                    .topic(topic)
                    .subscriptionName("gmTest")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageCrypto(new MessageCryptoGm4Consumer())
                    .cryptoKeyReader(new RawFileKeyReader(publicKeyPath, privateKeyPath))
                    .subscribe();

            Producer<String> producer = tlqcnClient.newProducer(Schema.STRING)
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
            log.info(FUNCTION_TEST, "---------------国密消息测试完毕---------------");
            consumer.close();
            producer.close();
        } catch (TlqcnClientException e) {
            log.error("测试程序出现异常", e);
        }
    }

    @Override
    public void gmTlsTest() {

    }

    @Override
    public void messagePriorityTest() {
        try {
            int totalPriority = 3;
            for (int i = 0; i < totalPriority; i++) {
                clearAndCreateTopic(topic + i);
            }
            PriorityUtil.startTest(totalPriority, topic, serviceUrl, true);
            for (int i = 0; i < totalPriority; i++) {
                clearAndCreateTopic(topic + i);
            }
            PriorityUtil.startTest(totalPriority, topic, serviceUrl, false);
        } catch (ExecutionException | InterruptedException | TlqcnClientException e) {
            log.error("测试程序出现异常", e);
        }
    }

    @Override
    public void failoverTest() {
        try {
            ServiceUrlProvider failover = AutoClusterFailover.builder()
                    .primary(tlqcnProperties.getClient().getFailoverConfig().getPrimaryUrl())
                    .secondary(List.of(tlqcnProperties.getClient().getFailoverConfig().getSecondaryUrl()))
                    .failoverDelay(10, TimeUnit.SECONDS)
                    .switchBackDelay(30, TimeUnit.SECONDS)
                    .checkInterval(1000, TimeUnit.MILLISECONDS)
                    .build();

            TlqcnClient client = TlqcnClient.builder()
                    .serviceUrlProvider(failover)
                    .build();

            failover.initialize(client);

            TlqcnAdmin primaryAdmin = TlqcnAdmin.builder()
                    .serviceHttpUrl(tlqcnProperties.getClient().getFailoverConfig().getPrimaryHttpUrl())
                    .build();

            TlqcnAdmin secondaryAdmin = TlqcnAdmin.builder()
                    .serviceHttpUrl(tlqcnProperties.getClient().getFailoverConfig().getSecondaryHttpUrl())
                    .build();

            try {
                primaryAdmin.topics().delete(topic, true);
                log.info("主集群删除topic<{}>成功", topic);
                secondaryAdmin.topics().delete(topic, true);
                log.info("备集群删除topic<{}>成功", topic);
            }catch (Exception e){
                log.info("删除topic<{}>失败", topic);
            }

            try {
                primaryAdmin.topics().createNonPartitionedTopic(topic);
                log.info("主集群创建topic<{}>成功", topic);
                secondaryAdmin.topics().createNonPartitionedTopic(topic);
                log.info("备集群创建topic<{}>成功", topic);
            }catch (Exception e){
                log.info("创建topic<{}>失败", topic);
            }

            Consumer<Long> failoverTestConsumer = client.newConsumer(Schema.INT64)
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("failoverSub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener((MessageListener<Long>) (consumer, msg) -> {
                        log.info(FUNCTION_TEST, "消费者<{}>消费消息<{}>，发送者<{}>,复制集群<{}>", consumer.getConsumerName(), msg.getValue(), msg.getProducerName(), msg.getReplicatedFrom());
                        //log.info("消费者<{}>消费消息<{}>，发送者<{}>,复制集群<{}>", consumer.getConsumerName(), msg.getValue(), msg.getProducerName(), msg.getReplicatedFrom());
                    })
                    .replicateSubscriptionState(true)
                    .ackTimeout(0,TimeUnit.SECONDS)
                    .subscribe();

            Producer<Long> producer = client.newProducer(Schema.INT64)
                    .topic(topic)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .create();

            for (long i = 0; i < msgNum; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
                //log.info("发送成功，内容<{}>, 消息id<{}>", i, send);
            }

            Thread.sleep(1000 * 13);
            log.info(FUNCTION_TEST, "---------------定时消息测试完毕---------------");
            producer.close();
            failoverTestConsumer.close();
            client.close();
        } catch (TlqcnClientException e) {
            log.error("测试程序出现异常", e);
        } /*catch (TlqcnAdminException e) {
            throw new RuntimeException(e);
        } */catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void transactionTest() {
        log.info(FUNCTION_TEST, "---------------事务测试开始---------------");
        String inputTopic = "inputTopic";
        String outputTopicOne = "outputTopicOne";
        String outputTopicTwo = "outputTopicTwo";
        clearAndCreateTopic(inputTopic);
        clearAndCreateTopic(outputTopicOne);
        clearAndCreateTopic(outputTopicTwo);
        TlqcnClient client;
        try {
            client = TlqcnClient.builder()
                    .serviceUrl(tlqcnProperties.getClient().getServiceUrl())
                    .enableTransaction(true)
                    .build();
            // create three producers to produce messages to input and output topics.
            ProducerBuilder<String> producerBuilder = client.newProducer(Schema.STRING);
            Producer<String> inputProducer = producerBuilder.topic(inputTopic)
                    .sendTimeout(0, TimeUnit.SECONDS).create();
            Producer<String> outputProducerOne = producerBuilder.topic(outputTopicOne)
                    .sendTimeout(0, TimeUnit.SECONDS).create();
            Producer<String> outputProducerTwo = producerBuilder.topic(outputTopicTwo)
                    .sendTimeout(0, TimeUnit.SECONDS).create();
            // create three consumers to consume messages from input and output topics.
            Consumer<String> inputConsumer = client.newConsumer(Schema.STRING)
                    .subscriptionName("sub").topic(inputTopic).subscribe();
            Consumer<String> outputConsumerOne = client.newConsumer(Schema.STRING)
                    .subscriptionName("sub").topic(outputTopicOne).subscribe();
            Consumer<String> outputConsumerTwo = client.newConsumer(Schema.STRING)
                    .subscriptionName("sub").topic(outputTopicTwo).subscribe();

            int count = 2;
            // produce messages to input topics.
            for (int i = 0; i < count; i++) {
                String msg = "Hello TongLINK/Q-CN! count : " + i;
                MessageId send = inputProducer.send(msg);
                log.info(FUNCTION_TEST, "向主题<{}>发送消息<{}>,消息id<{}>", inputTopic, msg, send);
            }

            // consume messages and produce them to output topics with transactions.
            for (int i = 0; i < count; i++) {

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
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // Final result: consume messages from output topics and print them.
            for (int i = 0; i < count; i++) {
                Message<String> message =  outputConsumerOne.receive();
                log.info(FUNCTION_TEST, "主题<{}>收取消息<{}>,消息id<{}>", outputTopicOne, message.getValue(), message.getMessageId());
                outputConsumerOne.acknowledge(message);
            }

            for (int i = 0; i < count; i++) {
                Message<String> message =  outputConsumerTwo.receive();
                log.info(FUNCTION_TEST, "主题<{}>收取消息<{}>,消息id<{}>", outputTopicTwo, message.getValue(), message.getMessageId());
                outputConsumerTwo.acknowledge(message);
            }
        } catch (TlqcnClientException e) {
            log.error("客户端创建失败", e);
        }
        log.info(FUNCTION_TEST, "--------------事务测试完毕---------------");
    }

    @Override
    public void batchSendTest() {
        List<Integer> batchSizes = tlqcnProperties.getFunctionTestConfig().getBatchSendSizes();
        if (batchSizes == null || batchSizes.isEmpty()) {
            batchSizes = Lists.newArrayList(500, 1000);
        }
        for (int batchSize : batchSizes) {
            batchSendTest(batchSize);
        }
    }

    /**
     * 批量发送消息测试
     *
     * @param batchSize 批量发送的消息数量
     */
    private void batchSendTest(int batchSize) {
        String batchTopic = topic + "_batch_" + batchSize;
        clearAndCreateTopic(batchTopic);
        log.info(FUNCTION_TEST, "--------批量发送消息测试开始，批量数量<{}>----------", batchSize);
        Producer<Long> producer = null;
        try {
            // 创建消费者
            CountDownLatch receiveLatch = new CountDownLatch(batchSize);
            AtomicInteger receiveCount = new AtomicInteger(0);
            Consumer<Long> consumer = createConsumer(batchTopic, null, "batch_sub",
                    "batch_consumer", SubscriptionType.Exclusive, Schema.INT64);

            // 覆写消费者的消息监听器以统计接收数量
            // 由于 createConsumer 使用了 messageListener，这里我们需要重新创建一个带计数的消费者
            consumer.close();

            Consumer<Long> batchConsumer = tlqcnClient.newConsumer(Schema.INT64)
                    .topic(batchTopic)
                    .consumerName("batch_consumer")
                    .subscriptionName("batch_sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener((MessageListener<Long>) (cons, msg) -> {
                        log.info(FUNCTION_TEST, "批量消费消息，批量数量<{}>, 消息内容<{}>", batchSize, msg.getValue());
                        try {
                            cons.acknowledge(msg);
                        } catch (TlqcnClientException e) {
                            log.error("消息确认异常", e);
                        }
                        receiveLatch.countDown();
                    })
                    .subscribe();

            Thread.sleep(3000);

            // 创建生产者并批量发送消息
            producer = createProducer(batchTopic, Schema.INT64);
            List<MessageId> messageIds = new ArrayList<>();
            for (long i = 0; i < batchSize; i++) {
                log.info(FUNCTION_TEST, "批量发送消息，批量数量<{}>, 消息内容<{}>", batchSize, i);
                MessageId send = producer.send(i);
                messageIds.add(send);
            }
            log.info(FUNCTION_TEST, "批量发送完毕，批量数量<{}>, 共发送<{}>条消息", batchSize, messageIds.size());

            // 等待消费者消费完毕
            boolean completed = receiveLatch.await(60, TimeUnit.SECONDS);
            int received = batchSize - (int) receiveLatch.getCount();
            if (completed && receiveLatch.getCount() == 0) {
                log.info(FUNCTION_TEST, "批量发送测试通过，批量数量<{}>, 发送<{}>条，消费<{}>条",
                        batchSize, messageIds.size(), received);
            } else {
                log.error(FUNCTION_TEST, "批量发送测试失败，批量数量<{}>, 发送<{}>条，消费<{}>条，超时未消费完",
                        batchSize, messageIds.size(), received);
            }

            batchConsumer.close();
            log.info(FUNCTION_TEST, "--------批量发送消息测试完毕，批量数量<{}>----------", batchSize);
        } catch (TlqcnClientException | InterruptedException e) {
            log.error("批量发送测试异常，批量数量<{}>", batchSize, e);
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
    }

    /**
     * 手动 ACK / NACK 测试：
     * 1）ACK 后消息从订阅中删除，不会再次投递
     * 2）NACK 后消息重新入队，在 negativeAckRedeliveryDelay 后再次投递
     */
    @Override
    public void manualAckTest() {
        String manualTopic = topic + "_manual_ack";
        clearAndCreateTopic(manualTopic);
        log.info(FUNCTION_TEST, "--------手动ACK/NACK测试开始----------");
        Producer<String> producer = null;
        Consumer<String> consumer = null;
        // NACK 后重新投递的等待时间（秒），receive 超时参数需要 int
        final int nackRedeliveryDelaySec = 5;
        try {
            // 不设置 messageListener，改为 receive 拉取，业务侧显式 ACK / NACK
            consumer = tlqcnClient.newConsumer(Schema.STRING)
                    .topic(manualTopic)
                    .consumerName("manual_ack_consumer")
                    .subscriptionName("manual_ack_sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .ackTimeout(30, TimeUnit.SECONDS)
                    .negativeAckRedeliveryDelay(nackRedeliveryDelaySec, TimeUnit.SECONDS)
                    .subscribe();
            log.info(FUNCTION_TEST, "创建消费者成功，negativeAckRedeliveryDelay={}s", nackRedeliveryDelaySec);

            Thread.sleep(2000);
            producer = createProducer(manualTopic, Schema.STRING);

            // ========== 阶段1：ACK → 消息删除，不再投递 ==========
            log.info(FUNCTION_TEST, "+++++阶段1：ACK 后删除消息+++++");
            String ackBody = "ack-then-delete-msg";
            MessageId ackSendId = producer.send(ackBody);
            log.info(FUNCTION_TEST, "发送待ACK消息，内容<{}>, 消息id<{}>", ackBody, ackSendId);

            Message<String> ackMsg = consumer.receive(10, TimeUnit.SECONDS);
            if (ackMsg == null) {
                log.error(FUNCTION_TEST, "阶段1失败，未收到待ACK消息");
            } else {
                log.info(FUNCTION_TEST, "收到消息（准备ACK），内容<{}>, 消息id<{}>, redeliveryCount<{}>",
                        ackMsg.getValue(), ackMsg.getMessageId(), ackMsg.getRedeliveryCount());
                consumer.acknowledge(ackMsg);
                log.info(FUNCTION_TEST, "已手动ACK，消息应从订阅中删除，内容<{}>, 消息id<{}>",
                        ackMsg.getValue(), ackMsg.getMessageId());

                // 等待超过 nack 重投时间，确认 ACK 后的消息不会再来
                log.info(FUNCTION_TEST, "等待{}s，验证ACK后的消息不会再次投递...", nackRedeliveryDelaySec + 2);
                Thread.sleep((nackRedeliveryDelaySec + 2) * 1000);

                Message<String> afterAck = consumer.receive(3, TimeUnit.SECONDS);
                if (afterAck == null) {
                    log.info(FUNCTION_TEST, "阶段1通过：ACK后未再收到消息，消息已删除");
                } else {
                    log.error(FUNCTION_TEST, "阶段1失败：ACK后仍收到消息，内容<{}>, 消息id<{}>, redeliveryCount<{}>",
                            afterAck.getValue(), afterAck.getMessageId(), afterAck.getRedeliveryCount());
                    // 清理意外收到的消息，避免干扰阶段2
                    consumer.acknowledge(afterAck);
                }
            }

            // ========== 阶段2：NACK → 重新入队并再次投递 ==========
            log.info(FUNCTION_TEST, "+++++阶段2：NACK 后重新入队+++++");
            String nackBody = "nack-then-requeue-msg";
            MessageId nackSendId = producer.send(nackBody);
            log.info(FUNCTION_TEST, "发送待NACK消息，内容<{}>, 消息id<{}>", nackBody, nackSendId);

            Message<String> nackMsg = consumer.receive(10, TimeUnit.SECONDS);
            if (nackMsg == null) {
                log.error(FUNCTION_TEST, "阶段2失败，未收到待NACK消息");
            } else {
                log.info(FUNCTION_TEST, "首次收到消息（准备NACK），内容<{}>, 消息id<{}>, redeliveryCount<{}>",
                        nackMsg.getValue(), nackMsg.getMessageId(), nackMsg.getRedeliveryCount());
                consumer.negativeAcknowledge(nackMsg);
                log.info(FUNCTION_TEST, "已手动NACK，消息将重新入队，内容<{}>, 消息id<{}>",
                        nackMsg.getValue(), nackMsg.getMessageId());

                log.info(FUNCTION_TEST, "等待 negativeAckRedeliveryDelay={}s，观察消息重新入队投递...",
                        nackRedeliveryDelaySec);
                Message<String> requeued = consumer.receive(nackRedeliveryDelaySec + 10, TimeUnit.SECONDS);
                if (requeued == null) {
                    log.error(FUNCTION_TEST, "阶段2失败：NACK后未收到重新入队的消息");
                } else {
                    log.info(FUNCTION_TEST, "收到重新入队消息，内容<{}>, 消息id<{}>, redeliveryCount<{}>",
                            requeued.getValue(), requeued.getMessageId(), requeued.getRedeliveryCount());
                    if (nackBody.equals(requeued.getValue())) {
                        log.info(FUNCTION_TEST, "阶段2通过：NACK后消息重新入队并再次投递");
                    } else {
                        log.error(FUNCTION_TEST, "阶段2异常：重投消息内容不匹配，期望<{}>, 实际<{}>",
                                nackBody, requeued.getValue());
                    }
                    // 最终 ACK，删除该消息，避免残留
                    consumer.acknowledge(requeued);
                    log.info(FUNCTION_TEST, "对重投消息执行ACK，消息删除，内容<{}>, 消息id<{}>",
                            requeued.getValue(), requeued.getMessageId());
                }
            }

            log.info(FUNCTION_TEST, "--------手动ACK/NACK测试完毕----------");
        } catch (TlqcnClientException | InterruptedException e) {
            log.error(FUNCTION_TEST, "手动ACK/NACK测试异常", e);
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                    log.info("消费者<{}>关闭成功", consumer.getConsumerName());
                }
            } catch (TlqcnClientException e) {
                log.error("消费者关闭异常", e);
            }
            try {
                if (producer != null) {
                    producer.close();
                    log.info("生产者关闭成功");
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
        }
    }

    /**
     * 自动删除订阅测试：
     * 1）NonDurable：最后消费者断开后，订阅自动删除
     * 2）Durable（对照）：消费者断开后，订阅仍然保留
     */
    @Override
    public void autoDeleteSubTest() {
        String autoDeleteTopic = topic + "_auto_delete_sub";
        clearAndCreateTopic(autoDeleteTopic);
        log.info(FUNCTION_TEST, "--------自动删除订阅测试开始----------");

        final String nonDurableSub = "auto_delete_nondurable_sub";
        final String durableSub = "auto_delete_durable_sub";
        Consumer<String> nonDurableConsumer = null;
        Consumer<String> durableConsumer = null;

        try {
            // ========== 阶段1：NonDurable 订阅，最后消费者断开后自动删除 ==========
            log.info(FUNCTION_TEST, "+++++阶段1：NonDurable 订阅，消费者断开后应自动删除+++++");
            nonDurableConsumer = tlqcnClient.newConsumer(Schema.STRING)
                    .topic(autoDeleteTopic)
                    .consumerName("auto_delete_nondurable_consumer")
                    .subscriptionName(nonDurableSub)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionMode(SubscriptionMode.NonDurable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            log.info(FUNCTION_TEST, "创建 NonDurable 消费者成功，subscription<{}>", nonDurableSub);

            // 给 broker 一点时间注册订阅
            Thread.sleep(2000);
            List<String> subsAfterCreate = tlqcnAdmin.topics().getSubscriptions(autoDeleteTopic);
            log.info(FUNCTION_TEST, "消费者在线时订阅列表: {}", subsAfterCreate);
            if (subsAfterCreate != null && subsAfterCreate.contains(nonDurableSub)) {
                log.info(FUNCTION_TEST, "NonDurable 订阅创建成功，subscription<{}> 存在", nonDurableSub);
            } else {
                // 部分实现上 NonDurable 可能不落持久元数据，这里仍继续验证断开后不存在
                log.warn(FUNCTION_TEST, "Admin 未列出 NonDurable 订阅（可能未持久化），继续验证断开后状态，当前列表: {}",
                        subsAfterCreate);
            }

            nonDurableConsumer.close();
            nonDurableConsumer = null;
            log.info(FUNCTION_TEST, "已关闭 NonDurable 消费者，等待订阅自动删除...");
            Thread.sleep(3000);

            List<String> subsAfterClose = tlqcnAdmin.topics().getSubscriptions(autoDeleteTopic);
            log.info(FUNCTION_TEST, "消费者断开后订阅列表: {}", subsAfterClose);
            if (subsAfterClose == null || !subsAfterClose.contains(nonDurableSub)) {
                log.info(FUNCTION_TEST, "阶段1通过：最后消费者断开后，NonDurable 订阅<{}>已自动删除", nonDurableSub);
            } else {
                log.error(FUNCTION_TEST, "阶段1失败：消费者断开后 NonDurable 订阅<{}>仍存在，列表: {}",
                        nonDurableSub, subsAfterClose);
            }

            // ========== 阶段2：Durable 订阅对照，断开后仍保留 ==========
            log.info(FUNCTION_TEST, "+++++阶段2：Durable 订阅对照，消费者断开后应保留+++++");
            durableConsumer = tlqcnClient.newConsumer(Schema.STRING)
                    .topic(autoDeleteTopic)
                    .consumerName("auto_delete_durable_consumer")
                    .subscriptionName(durableSub)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionMode(SubscriptionMode.Durable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            log.info(FUNCTION_TEST, "创建 Durable 消费者成功，subscription<{}>", durableSub);

            Thread.sleep(2000);
            List<String> durableSubsOnline = tlqcnAdmin.topics().getSubscriptions(autoDeleteTopic);
            log.info(FUNCTION_TEST, "Durable 消费者在线时订阅列表: {}", durableSubsOnline);
            if (durableSubsOnline == null || !durableSubsOnline.contains(durableSub)) {
                log.error(FUNCTION_TEST, "阶段2准备失败：Durable 订阅<{}>未出现在列表中: {}",
                        durableSub, durableSubsOnline);
            }

            durableConsumer.close();
            durableConsumer = null;
            log.info(FUNCTION_TEST, "已关闭 Durable 消费者，验证订阅是否保留...");
            Thread.sleep(3000);

            List<String> durableSubsAfterClose = tlqcnAdmin.topics().getSubscriptions(autoDeleteTopic);
            log.info(FUNCTION_TEST, "Durable 消费者断开后订阅列表: {}", durableSubsAfterClose);
            if (durableSubsAfterClose != null && durableSubsAfterClose.contains(durableSub)) {
                log.info(FUNCTION_TEST, "阶段2通过：Durable 订阅<{}>在消费者断开后仍然保留", durableSub);
            } else {
                log.error(FUNCTION_TEST, "阶段2失败：Durable 订阅<{}>在消费者断开后消失，列表: {}",
                        durableSub, durableSubsAfterClose);
            }

            // 清理 Durable 订阅残留
            try {
                tlqcnAdmin.topics().deleteSubscription(autoDeleteTopic, durableSub, true);
                log.info(FUNCTION_TEST, "清理 Durable 订阅<{}>成功", durableSub);
            } catch (TlqcnAdminException e) {
                log.warn(FUNCTION_TEST, "清理 Durable 订阅<{}>失败（可忽略）: {}", durableSub, e.getMessage());
            }

            log.info(FUNCTION_TEST, "--------自动删除订阅测试完毕----------");
        } catch (TlqcnClientException | InterruptedException | TlqcnAdminException e) {
            log.error(FUNCTION_TEST, "自动删除订阅测试异常", e);
        } finally {
            try {
                if (nonDurableConsumer != null) {
                    nonDurableConsumer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("NonDurable 消费者关闭异常", e);
            }
            try {
                if (durableConsumer != null) {
                    durableConsumer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("Durable 消费者关闭异常", e);
            }
        }
    }

    /**
     * 消息超长拒绝测试：
     * 1）设置 topic 的 maxMessageSize
     * 2）不超过限制的消息发送成功
     * 3）超过限制的消息被拒绝（send 抛异常）
     * 4）拒绝后，合法消息仍可正常发送
     */
    @Override
    public void maxMessageSizeRejectTest() {
        String sizeTopic = topic + "_max_msg_size";
        clearAndCreateTopic(sizeTopic);
        log.info(FUNCTION_TEST, "--------消息超长拒绝测试开始----------");

        // 限制单条消息最大 1024 字节
        final int maxMessageSize = 1024;
        Producer<byte[]> producer = null;
        try {
            tlqcnAdmin.topicPolicies().setMaxMessageSize(sizeTopic, maxMessageSize);
            log.info(FUNCTION_TEST, "已设置 topic<{}> maxMessageSize={} 字节", sizeTopic, maxMessageSize);

            // 等待策略生效
            Thread.sleep(3000);
            Integer applied = null;
            try {
                applied = tlqcnAdmin.topicPolicies().getMaxMessageSize(sizeTopic);
            } catch (TlqcnAdminException e) {
                log.warn(FUNCTION_TEST, "读取 maxMessageSize 失败（继续测试）: {}", e.getMessage());
            }
            log.info(FUNCTION_TEST, "当前生效 maxMessageSize={}", applied);

            // 关闭 batching / chunking，确保超长消息不会被拆包绕过限制
            producer = tlqcnClient.newProducer(Schema.BYTES)
                    .topic(sizeTopic)
                    .enableBatching(false)
                    .enableChunking(false)
                    .sendTimeout(5, TimeUnit.SECONDS)
                    .create();

            // 阶段1：合法消息（小于限制）应发送成功
            log.info(FUNCTION_TEST, "+++++阶段1：发送不超过限制的消息，应成功+++++");
            byte[] okPayload = new byte[maxMessageSize / 2];
            for (int i = 0; i < okPayload.length; i++) {
                okPayload[i] = (byte) ('A' + (i % 26));
            }
            try {
                MessageId okId = producer.send(okPayload);
                log.info(FUNCTION_TEST, "阶段1通过：合法消息发送成功，size={}字节, messageId<{}>",
                        okPayload.length, okId);
            } catch (TlqcnClientException e) {
                log.error(FUNCTION_TEST, "阶段1失败：合法消息发送被拒绝，size={}字节", okPayload.length, e);
            }

            // 阶段2：超长消息应被拒绝
            log.info(FUNCTION_TEST, "+++++阶段2：发送超过限制的消息，应按拒绝策略处理+++++");
            byte[] overPayload = new byte[maxMessageSize + 1];
            for (int i = 0; i < overPayload.length; i++) {
                overPayload[i] = (byte) ('a' + (i % 26));
            }
            try {
                MessageId overId = producer.send(overPayload);
                log.error(FUNCTION_TEST, "阶段2失败：超长消息未被拒绝，size={}字节, messageId<{}>",
                        overPayload.length, overId);
            } catch (TlqcnClientException e) {
                log.info(FUNCTION_TEST, "阶段2通过：超长消息已被拒绝，size={}字节, maxMessageSize={}, 异常: {}",
                        overPayload.length, maxMessageSize, e.getMessage());
            }

            // 阶段3：拒绝后，合法消息仍应可发送（broker/producer 未被异常状态污染）
            log.info(FUNCTION_TEST, "+++++阶段3：拒绝后再次发送合法消息，应成功+++++");
            byte[] okPayload2 = new byte[64];
            for (int i = 0; i < okPayload2.length; i++) {
                okPayload2[i] = (byte) i;
            }
            try {
                MessageId okId2 = producer.send(okPayload2);
                log.info(FUNCTION_TEST, "阶段3通过：拒绝后合法消息仍可发送，size={}字节, messageId<{}>",
                        okPayload2.length, okId2);
            } catch (TlqcnClientException e) {
                log.error(FUNCTION_TEST, "阶段3失败：拒绝后合法消息无法发送，size={}字节", okPayload2.length, e);
            }

            log.info(FUNCTION_TEST, "--------消息超长拒绝测试完毕----------");
        } catch (TlqcnAdminException e) {
            log.error(FUNCTION_TEST, "设置/读取 maxMessageSize 失败", e);
        } catch (TlqcnClientException | InterruptedException e) {
            log.error(FUNCTION_TEST, "消息超长拒绝测试异常", e);
        } finally {
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
            try {
                tlqcnAdmin.topicPolicies().removeMaxMessageSize(sizeTopic);
                log.info(FUNCTION_TEST, "已清理 topic<{}> maxMessageSize 策略", sizeTopic);
            } catch (TlqcnAdminException e) {
                log.warn(FUNCTION_TEST, "清理 maxMessageSize 失败（可忽略）: {}", e.getMessage());
            }
        }
    }

    /**
     * 背压测试（生产超过处理速率）：
     * 1）Producer 使用较小 maxPendingMessages + blockIfQueueFull
     * 2）Consumer 故意慢消费，制造处理不过来
     * 3）持续高速发送一段时间，采样堆内存
     * 预期：触发背压（send 阻塞变慢），堆内存有上界，进程不 OOM
     * 参数见 application.yml → tlqcn.function-test-config.backpressure-test-config
     */
    @Override
    public void backpressureTest() {
        String bpTopic = topic + "_backpressure";
        clearAndCreateTopic(bpTopic);
        log.info(FUNCTION_TEST, "--------背压测试开始----------");

        TlqcnProperties.BackpressureTestConfig bpCfg = tlqcnProperties.getFunctionTestConfig().getBackpressureTestConfig();
        if (bpCfg == null) {
            bpCfg = new TlqcnProperties.BackpressureTestConfig();
            log.warn(FUNCTION_TEST, "未配置 backpressure-test-config，使用默认参数");
        }

        final int maxPendingMessages = bpCfg.getMaxPendingMessages() > 0 ? bpCfg.getMaxPendingMessages() : 50;
        final int msgSize = bpCfg.getMsgSize() > 0 ? bpCfg.getMsgSize() : 32768;
        final long testDurationMs = bpCfg.getTestDurationMs() > 0 ? bpCfg.getTestDurationMs() : 60_000L;
        final long slowConsumeSleepMs = bpCfg.getSlowConsumeSleepMs() >= 0 ? bpCfg.getSlowConsumeSleepMs() : 200L;
        final int receiverQueueSize = bpCfg.getReceiverQueueSize() > 0 ? bpCfg.getReceiverQueueSize() : 10;
        final long heapGrowthLimitBytes = (bpCfg.getHeapGrowthLimitMb() > 0 ? bpCfg.getHeapGrowthLimitMb() : 128L) * 1024 * 1024;
        final double heapMaxUsageRatio = bpCfg.getHeapMaxUsageRatio() > 0 ? bpCfg.getHeapMaxUsageRatio() : 0.80;
        final boolean blockIfQueueFull = bpCfg.isBlockIfQueueFull();
        final int sendTimeoutSec = bpCfg.getSendTimeoutSec() > 0 ? bpCfg.getSendTimeoutSec() : 30;
        final long heapSampleIntervalMs = bpCfg.getHeapSampleIntervalMs() > 0 ? bpCfg.getHeapSampleIntervalMs() : 2000L;
        final long blockThresholdMs = bpCfg.getBlockThresholdMs() > 0 ? bpCfg.getBlockThresholdMs() : 100L;
        final long avgSendCostThresholdMs = bpCfg.getAvgSendCostThresholdMs() > 0 ? bpCfg.getAvgSendCostThresholdMs() : 50L;

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long baselineHeap = memoryMXBean.getHeapMemoryUsage().getUsed();
        long maxHeap = memoryMXBean.getHeapMemoryUsage().getMax();
        log.info(FUNCTION_TEST,
                "背压参数: maxPending={}, msgSize={}KB, duration={}s, slowConsumeSleep={}ms, receiverQueueSize={}, "
                        + "blockIfQueueFull={}, sendTimeout={}s, heapGrowthLimit={}MB, heapMaxUsageRatio={}, "
                        + "blockThreshold={}ms, avgSendCostThreshold={}ms",
                maxPendingMessages, msgSize / 1024, testDurationMs / 1000, slowConsumeSleepMs, receiverQueueSize,
                blockIfQueueFull, sendTimeoutSec, heapGrowthLimitBytes / (1024 * 1024), heapMaxUsageRatio,
                blockThresholdMs, avgSendCostThresholdMs);
        log.info(FUNCTION_TEST, "堆基线 used={}MB, max={}MB",
                baselineHeap / (1024 * 1024),
                maxHeap > 0 ? maxHeap / (1024 * 1024) : -1);

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong sendSuccess = new AtomicLong(0);
        AtomicLong sendFail = new AtomicLong(0);
        AtomicLong sendBlockedCount = new AtomicLong(0);
        AtomicLong totalSendCostMs = new AtomicLong(0);
        AtomicLong consumeCount = new AtomicLong(0);
        AtomicLong peakHeap = new AtomicLong(baselineHeap);
        AtomicInteger oomFlag = new AtomicInteger(0);
        // 慢消费 listener 在途数量，关闭前先排空，避免 close 后 ACK 报 State: Closed
        AtomicInteger inflightConsume = new AtomicInteger(0);

        Thread producerThread = null;
        Thread monitorThread = null;

        try {
            // 慢消费者：拉到消息后 sleep，模拟处理速率不足
            consumer = tlqcnClient.newConsumer(Schema.BYTES)
                    .topic(bpTopic)
                    .consumerName("backpressure_consumer")
                    .subscriptionName("backpressure_sub")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .receiverQueueSize(receiverQueueSize)
                    .messageListener((MessageListener<byte[]>) (c, msg) -> {
                        // 测试已结束则不再处理，避免 close 后 ACK
                        if (!running.get()) {
                            return;
                        }
                        inflightConsume.incrementAndGet();
                        try {
                            if (slowConsumeSleepMs > 0) {
                                Thread.sleep(slowConsumeSleepMs);
                            }
                            // sleep 期间可能已经开始收尾
                            if (!running.get()) {
                                return;
                            }
                            c.acknowledge(msg);
                            consumeCount.incrementAndGet();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } catch (TlqcnClientException e) {
                            // 收尾阶段 consumer 已关闭是预期竞态，不打 ERROR
                            if (!running.get() || isConsumerClosedException(e)) {
                                log.debug(FUNCTION_TEST, "慢消费 ACK 跳过（consumer 已关闭/测试结束）: {}",
                                        e.getMessage());
                            } else {
                                log.error(FUNCTION_TEST, "慢消费 ACK 异常", e);
                            }
                        } finally {
                            inflightConsume.decrementAndGet();
                        }
                    })
                    .subscribe();
            log.info(FUNCTION_TEST, "慢消费者已启动，处理间隔约 {}ms, receiverQueueSize={}",
                    slowConsumeSleepMs, receiverQueueSize);

            producer = tlqcnClient.newProducer(Schema.BYTES)
                    .topic(bpTopic)
                    .enableBatching(false)
                    .enableChunking(false)
                    .maxPendingMessages(maxPendingMessages)
                    .blockIfQueueFull(blockIfQueueFull)
                    .sendTimeout(sendTimeoutSec, TimeUnit.SECONDS)
                    .create();
            log.info(FUNCTION_TEST, "Producer 已创建: maxPendingMessages={}, blockIfQueueFull={}",
                    maxPendingMessages, blockIfQueueFull);

            Thread.sleep(2000);

            byte[] payload = new byte[msgSize];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (i % 256);
            }

            // 堆采样线程
            monitorThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        long used = memoryMXBean.getHeapMemoryUsage().getUsed();
                        peakHeap.accumulateAndGet(used, Math::max);
                        log.info(FUNCTION_TEST,
                                "堆采样 used={}MB, peak={}MB, sendOk={}, sendFail={}, blocked(>{}ms)={}, consumed={}",
                                used / (1024 * 1024),
                                peakHeap.get() / (1024 * 1024),
                                sendSuccess.get(),
                                sendFail.get(),
                                blockThresholdMs,
                                sendBlockedCount.get(),
                                consumeCount.get());
                        Thread.sleep(heapSampleIntervalMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (OutOfMemoryError oom) {
                        oomFlag.set(1);
                        log.error(FUNCTION_TEST, "监控线程捕获 OOM", oom);
                        break;
                    }
                }
            }, "backpressure-heap-monitor");
            monitorThread.setDaemon(true);
            monitorThread.start();

            // 高速生产线程：同步 send，blockIfQueueFull 时会阻塞
            Producer<byte[]> finalProducer = producer;
            producerThread = new Thread(() -> {
                long deadline = System.currentTimeMillis() + testDurationMs;
                while (running.get() && System.currentTimeMillis() < deadline) {
                    long start = System.currentTimeMillis();
                    try {
                        finalProducer.send(payload);
                        long cost = System.currentTimeMillis() - start;
                        totalSendCostMs.addAndGet(cost);
                        sendSuccess.incrementAndGet();
                        if (cost > blockThresholdMs) {
                            sendBlockedCount.incrementAndGet();
                        }
                    } catch (TlqcnClientException e) {
                        long cost = System.currentTimeMillis() - start;
                        totalSendCostMs.addAndGet(cost);
                        sendFail.incrementAndGet();
                        if (cost > blockThresholdMs) {
                            sendBlockedCount.incrementAndGet();
                        }
                        log.warn(FUNCTION_TEST, "发送失败（可能是超时/背压相关）: {}", e.getMessage());
                    } catch (OutOfMemoryError oom) {
                        oomFlag.set(1);
                        log.error(FUNCTION_TEST, "生产线程捕获 OOM", oom);
                        break;
                    }
                }
            }, "backpressure-producer");
            producerThread.start();

            log.info(FUNCTION_TEST, "开始高压发送，持续 {} 秒...", testDurationMs / 1000);
            producerThread.join(testDurationMs + 30_000);
            running.set(false);
            if (monitorThread.isAlive()) {
                monitorThread.join(5000);
            }

            long finalHeap = memoryMXBean.getHeapMemoryUsage().getUsed();
            long peak = peakHeap.get();
            long growth = Math.max(0, peak - baselineHeap);
            long success = sendSuccess.get();
            long fail = sendFail.get();
            long blocked = sendBlockedCount.get();
            long consumed = consumeCount.get();
            long avgSendCost = success + fail > 0 ? totalSendCostMs.get() / (success + fail) : 0;

            log.info(FUNCTION_TEST, "压测统计: sendOk={}, sendFail={}, blocked(>{}ms)={}, consumed={}, avgSendCost={}ms",
                    success, fail, blockThresholdMs, blocked, consumed, avgSendCost);
            log.info(FUNCTION_TEST, "内存统计: baseline={}MB, peak={}MB, final={}MB, growth={}MB",
                    baselineHeap / (1024 * 1024),
                    peak / (1024 * 1024),
                    finalHeap / (1024 * 1024),
                    growth / (1024 * 1024));

            // ---- 判定 ----
            boolean noOom = oomFlag.get() == 0;
            boolean heapBounded = growth <= heapGrowthLimitBytes
                    && (maxHeap <= 0 || peak <= (long) (maxHeap * heapMaxUsageRatio));
            boolean backpressureObserved = blocked > 0 || avgSendCost > avgSendCostThresholdMs || fail > 0;
            boolean overload = success > consumed;

            if (noOom) {
                log.info(FUNCTION_TEST, "判定-无OOM: 通过");
            } else {
                log.error(FUNCTION_TEST, "判定-无OOM: 失败，检测到 OutOfMemoryError");
            }
            if (heapBounded) {
                log.info(FUNCTION_TEST, "判定-堆有上界: 通过, growth={}MB <= limit={}MB",
                        growth / (1024 * 1024), heapGrowthLimitBytes / (1024 * 1024));
            } else {
                log.error(FUNCTION_TEST, "判定-堆有上界: 失败, peak={}MB, growth={}MB, maxHeap={}MB",
                        peak / (1024 * 1024), growth / (1024 * 1024),
                        maxHeap > 0 ? maxHeap / (1024 * 1024) : -1);
            }
            if (backpressureObserved) {
                log.info(FUNCTION_TEST, "判定-背压触发: 通过, blocked={}, avgSendCost={}ms, fail={}",
                        blocked, avgSendCost, fail);
            } else {
                log.error(FUNCTION_TEST, "判定-背压触发: 失败，未观察到阻塞/变慢/受控失败（可调小 max-pending-messages 或加大 slow-consume-sleep-ms）");
            }
            if (overload) {
                log.info(FUNCTION_TEST, "判定-超过处理速率: 通过, sendOk={} > consumed={}", success, consumed);
            } else {
                log.warn(FUNCTION_TEST, "判定-超过处理速率: 未明显成立, sendOk={}, consumed={}（可增大 slow-consume-sleep-ms）",
                        success, consumed);
            }

            if (noOom && heapBounded && backpressureObserved) {
                log.info(FUNCTION_TEST, "--------背压测试通过：系统稳定，未OOM，背压生效----------");
            } else {
                log.error(FUNCTION_TEST, "--------背压测试未通过，请检查上述判定项----------");
            }
        } catch (TlqcnClientException | InterruptedException e) {
            log.error(FUNCTION_TEST, "背压测试异常", e);
        } finally {
            // 1) 先停生产循环与采样
            running.set(false);
            try {
                if (producerThread != null && producerThread.isAlive()) {
                    producerThread.join(3000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // 2) 先关 producer，避免继续推消息给 listener
            try {
                if (producer != null) {
                    producer.close();
                    producer = null;
                }
            } catch (TlqcnClientException e) {
                log.error("生产者关闭异常", e);
            }
            // 3) 等待在途慢消费结束
            long drainDeadline = System.currentTimeMillis()
                    + Math.max(slowConsumeSleepMs * 2, 3000L) + 2000L;
            while (inflightConsume.get() > 0 && System.currentTimeMillis() < drainDeadline) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (inflightConsume.get() > 0) {
                log.warn(FUNCTION_TEST, "关闭前仍有在途消费 {} 条，继续关闭 consumer",
                        inflightConsume.get());
            }
            // 4) 再关 consumer
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (TlqcnClientException e) {
                log.error("消费者关闭异常", e);
            }
            log.info(FUNCTION_TEST, "--------背压测试完毕----------");
        }
    }

    /** 判断是否为 consumer 已关闭导致的 ACK 异常（收尾竞态） */
    private static boolean isConsumerClosedException(Throwable e) {
        Throwable t = e;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("State: Closed") || msg.contains("Consumer not ready"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
