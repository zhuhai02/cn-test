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
import com.tongtech.tlqcn.shade.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class FunctionTestServiceImpl implements FunctionTestService {
    private static final Logger log = LoggerFactory.getLogger("FUNCTION_TEST_LOGGER");
    private static final Marker FUNCTION_TEST = MarkerFactory.getMarker("FUNCTION_TEST") ;

    private final TlqcnClient tlqcnClient;
    private final TlqcnAdmin tlqcnAdmin;
    private final TlqcnProperties tlqcnProperties;

    private final String topic;
    private final String publicKeyPath;
    private final String privateKeyPath;
    private final String serviceUrl;

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
    }

    /**
     * 测试前先清理topic
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

    private  <T> Producer<T> createProducer(String topic, Schema<T> schema) throws TlqcnClientException {
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
        if (tlqcnProperties.getFunctionTestConfig().isEnabledSyncSendTest()) {
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
    }

    @Override
    public void syncSendTest() {
        clearAndCreateTopic(topic);
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "--------同步发送消息测试开始----------");
        try {
            producer = createProducer(topic, Schema.INT64);
            for (long i = 0; i < 10 ; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                MessageId send = producer.send(i);
                log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", i, send);
            }
            log.info(FUNCTION_TEST, "--------同步发送消息测试完毕----------");
        } catch (TlqcnClientException e) {
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
    public void asyncSendTest() {
        clearAndCreateTopic(topic);
        Producer<Long> producer = null;
        log.info(FUNCTION_TEST, "--------异步发送消息测试开始----------");
        try {
            producer = createProducer(topic, Schema.INT64);
            CountDownLatch countDownLatch = new CountDownLatch(10);
            for (long i = 0; i < 10 ; i++) {
                log.info(FUNCTION_TEST, "发送消息，内容<{}>", i);
                long finalI = i;
                producer.sendAsync(i)
                        .thenAccept(send -> {
                            log.info(FUNCTION_TEST, "发送成功，内容<{}>, 消息id<{}>", finalI, send);
                            countDownLatch.countDown();
                        })
                        .exceptionally(throwable -> {
                            log.error("发送异常", throwable);
                            countDownLatch.countDown();
                            return null;
                        });

            }
            countDownLatch.await();
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
        subscribeTest("Exclusive_sub", SubscriptionType.Exclusive);
        subscribeTest("Failover_sub", SubscriptionType.Failover);
        subscribeTest("Shared_sub", SubscriptionType.Shared);
        subscribeTest("Key_Shared_sub", SubscriptionType.Key_Shared);
        log.info(FUNCTION_TEST, "--------订阅类型测试完毕----------");
    }

    /**
     * 独占订阅测试
     */
    private void subscribeTest(String subName, SubscriptionType subscriptionType) {
        clearAndCreateTopic(topic);
        try {
            log.info(FUNCTION_TEST, "+++++++++++{}订阅类型测试开始+++++++++++", subscriptionType);
            List<Consumer<Long>> consumers = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
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
            for (long i = 0; i < 100 ; i++) {
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
            for (long i = 0; i < 10 ; i++) {
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
            if (producer!= null) {
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
            for (long i = 0; i < 10 ; i++) {
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
            if (producer!= null) {
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
            for (int i = 0; i < 1; i++) {
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
        if (deadLetterTopic!= null) {
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
            long delay = 10;
            MessageId send1 = producer.newMessage()
                    .deliverAfter(delay, TimeUnit.SECONDS)
                    .value("延时消息!")
                    .send();
            log.info(FUNCTION_TEST, "延时消息发送成功，延时<{}>秒,消息id<{}>", delay, send1);

            Thread.sleep(1000 * 13);
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

            for (long i = 0; i < 10 ; i++) {
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
}
