package com.tongtech.cntest.service;

import com.tongtech.tlqcn.client.admin.TlqcnAdmin;
import com.tongtech.tlqcn.client.api.*;
import com.tongtech.tlqcn.shade.io.netty.channel.DefaultEventLoop;
import com.tongtech.tlqcn.shade.io.netty.channel.EventLoop;
import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.PerfTestService;
import com.tongtech.cntest.utils.PaddingDecimalFormat;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class PerfTestServiceImpl implements PerfTestService {

    private static final Logger log = LoggerFactory.getLogger(PerfTestServiceImpl.class);
    private static final Logger producerLog = LoggerFactory.getLogger("PERF_PRODUCER_TEST_LOGGER");
    private static final Logger consumerLog = LoggerFactory.getLogger("PERF_CONSUMER_TEST_LOGGER");
    private static final Marker PERF_PRODUCER_TEST = MarkerFactory.getMarker("PERF_PRODUCER_TEST") ;
    private static final Marker PERF_CONSUMER_TEST = MarkerFactory.getMarker("PERF_CONSUMER_TEST") ;

    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();
    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    static final DecimalFormat THROUGHPUTFORMAT = new PaddingDecimalFormat("0.0", 8);
    private final AtomicBoolean isStop = new AtomicBoolean(false);

    private final AtomicBoolean isTesting = new AtomicBoolean(false);

    private final String topic;
    private final boolean enabled;
    private final long batchingMaxPublishDelay;
    private final long testTimePerCase;
    private final int batchingMaxMessages;
    private final long logInterval;
    private final long caseInterval;
    private final long closeTime;

    private final ExecutorService perfTestExecutorService;
    private final TlqcnClient tlqcnClient;
    private final TlqcnAdmin tlqcnAdmin;
    private final TlqcnProperties tlqcnProperties;

    @Autowired
    public PerfTestServiceImpl(TlqcnClient tlqcnClient, TlqcnAdmin tlqcnAdmin,
                               ExecutorService perfTestExecutorService,
                               TlqcnProperties tlqcnProperties) {
        this.tlqcnClient = tlqcnClient;
        this.tlqcnAdmin = tlqcnAdmin;
        this.perfTestExecutorService = perfTestExecutorService;
        this.tlqcnProperties = tlqcnProperties;
        this.topic = tlqcnProperties.getPerfParamConfigs().getTopic();
        this.enabled = tlqcnProperties.getPerfParamConfigs().isEnabled();
        this.batchingMaxPublishDelay= tlqcnProperties.getPerfParamConfigs()
                .getBatchingMaxPublishDelay();
        this.testTimePerCase= tlqcnProperties.getPerfParamConfigs().getTestTimePerCase();
        this.batchingMaxMessages= tlqcnProperties.getPerfParamConfigs().getBatchingMaxMessages();
        this.logInterval= tlqcnProperties.getPerfParamConfigs().getLogInterval();
        this.caseInterval= tlqcnProperties.getPerfParamConfigs().getCaseInterval();
        this.closeTime= tlqcnProperties.getPerfParamConfigs().getCloseTime();
    }

    /**
     * 测试前先清理topic
     */
    private void clearAndCreateTopic(int partitionNum) {
//        try {
//            tlqcnAdmin.topics().deletePartitionedTopic(topic, true);
//            log.info("删除topic<{}>成功", topic);
//        } catch (TlqcnAdminException e) {
//        }
//
//        try {
//            tlqcnAdmin.topics().createPartitionedTopic(topic, partitionNum);
//            log.info("创建topic<{}>成功", topic);
//        } catch (TlqcnAdminException e) {
//            log.error("创建topic<{}>异常", topic, e);
//        }
    }


    private Producer<byte[]> createProducer() {
        try {
            return tlqcnClient.newProducer(Schema.BYTES)
                    .topic(topic)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .batchingMaxPublishDelay(batchingMaxPublishDelay, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(batchingMaxMessages)
                    .batchingMaxBytes(5 * 1024 * 1024)
                    .blockIfQueueFull(true)
                    .create();
        } catch (TlqcnClientException e) {
            log.error("创建producer异常", e);
        }
        return null;
    }


    @Override
    @Scheduled(cron = "${tlqcn.perf-param-configs.cron}")
    public void startTest() {
        if (!enabled) {
            log.info("性能测试未开启");
            return;
        }
        isTesting.set(true);
        perfTestExecutorService.execute(() -> {
            long oldTime = System.nanoTime();
            while (isTesting.get()) {
                try {
                    Thread.sleep(logInterval);
                } catch (InterruptedException e) {
                    break;
                }

                long now = System.nanoTime();
                double elapsed = (now - oldTime) / 1e9;
                double sentRate = messagesSent.sumThenReset() / elapsed;
                double sentThroughput = bytesSent.sumThenReset() / elapsed / 1024 / 1024;

                double receivedRate = messagesReceived.sumThenReset() / elapsed;
                double receivedThroughput = bytesReceived.sumThenReset() / elapsed / 1024 / 1024;

                producerLog.info(PERF_PRODUCER_TEST,
                        "生产指标: {} msg/s --- {} MB/s",
                        THROUGHPUTFORMAT.format(sentRate), THROUGHPUTFORMAT.format(sentThroughput));

                consumerLog.info(PERF_CONSUMER_TEST,
                        "消费指标: {}  msg/s --- {} MB/s ",
                        THROUGHPUTFORMAT.format(receivedRate), THROUGHPUTFORMAT.format(receivedThroughput));

                oldTime = now;
            }
        });

        if (tlqcnProperties.getPerfParamConfigs().isEnablePerfSyncTest()) {
            startTest(true);
        }

        if (tlqcnProperties.getPerfParamConfigs().isEnablePerfAsyncTest()) {
            startTest(false);
        }
        isTesting.set(false);
    }

    private void startTest(boolean isSyncTest) {
        for (TlqcnProperties.PerfCaseConfig config : tlqcnProperties.getPerfCaseConfigs()) {
            if (isSyncTest) {
                producerLog.info(PERF_PRODUCER_TEST, "同步开始测试，生产者数量：{}，消费者数量：{}，主题分区数量：{}，消息大小：{}，每个生产者线程数量：{}",
                        config.getProducerNum(), config.getConsumerNum(), config.getTopicPartitionNum(),
                        config.getMsgSize(), config.getPerProducerThreadNum());
                consumerLog.info(PERF_CONSUMER_TEST, "同步开始测试，生产者数量：{}，消费者数量：{}，主题分区数量：{}，消息大小：{}，每个生产者线程数量：{}",
                        config.getProducerNum(), config.getConsumerNum(), config.getTopicPartitionNum(),
                        config.getMsgSize(), config.getPerProducerThreadNum());
                syncPerfTest(config.getProducerNum(), config.getConsumerNum(), config.getTopicPartitionNum(),
                        config.getMsgSize(), config.getPerProducerThreadNum(), config.isConsumeAfterSend(),
                        config.isSyncAck());
            } else {
                producerLog.info(PERF_PRODUCER_TEST, "异步开始测试，生产者数量：{}，消费者数量：{}，主题分区数量：{}，消息大小：{}，每个生产者线程数量：{}",
                        config.getProducerNum(), config.getConsumerNum(), config.getTopicPartitionNum(),
                        config.getMsgSize(), config.getPerProducerThreadNum());

                consumerLog.info(PERF_CONSUMER_TEST, "异步开始测试，生产者数量：{}，消费者数量：{}，主题分区数量：{}，消息大小：{}，每个生产者线程数量：{}",
                        config.getProducerNum(), config.getConsumerNum(), config.getTopicPartitionNum(),
                        config.getMsgSize(), config.getPerProducerThreadNum());
                asyncPerfTest(config.getProducerNum(), config.getConsumerNum(), config.getTopicPartitionNum(),
                        config.getMsgSize(), config.getPerProducerThreadNum(), config.isConsumeAfterSend(),
                        config.isSyncAck());
            }
            try {
                Thread.sleep(caseInterval);
            } catch (InterruptedException e) {
                log.error("测试线程sleep异常", e);
            }
        }
    }

    @Override
    public void syncPerfTest(int producerNum, int consumerNum, int topicPartitionNum, int msgSize,
                             int perProducerThreadNum, boolean consumeAfterSend, boolean syncAck) {
        clearAndCreateTopic(topicPartitionNum);
        // 打开生产消费开关
        isStop.set(false);
        // 线程池提交发送消息任务
        for (int i = 0; i < producerNum; i++) {
            perfTestExecutorService.execute(() -> syncSend(perProducerThreadNum, msgSize));
        }
        long testTime = testTimePerCase;
        // 如果是先生产后消费，消费线程使用延迟任务提交
        if (consumeAfterSend) {
            testTime += caseInterval;
            EventLoop eventLoop = new DefaultEventLoop();
            eventLoop.schedule(() -> {
                // 先关闭生产者
                isStop.set(true);
                try {
                    Thread.sleep(caseInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // 打开消费者
                isStop.set(false);
                // 提交消费任务
                for (int i = 0; i < consumerNum; i++) {
                    perfTestExecutorService.execute(() -> startConsume(syncAck));
                }
            }, testTimePerCase / 2, TimeUnit.MINUTES);
        } else {
            for (int i = 0; i < consumerNum; i++) {
                perfTestExecutorService.execute(() -> startConsume(syncAck));
            }
        }

        try {
            Thread.sleep(testTime);
        } catch (InterruptedException e) {
            log.error("测试线程sleep异常", e);
        }
        isStop.set(true);

        // 停止2分钟待生产者和消费者关闭
        try {
            Thread.sleep(closeTime);
        } catch (InterruptedException e) {
            log.error("测试线程sleep异常", e);
        }
    }


    @Override
    public void asyncPerfTest(int producerNum, int consumerNum, int topicPartitionNum, int msgSize,
                              int perProducerThreadNum, boolean consumeAfterSend, boolean syncAck) {
        clearAndCreateTopic(topicPartitionNum);
        // 打开生产消费开关
        isStop.set(false);
        // 线程池提交发送消息任务
        for (int i = 0; i < producerNum; i++) {
            perfTestExecutorService.execute(() -> asyncSend(perProducerThreadNum, msgSize));
        }
        long testTime = testTimePerCase;
        // 如果是先生产后消费，消费线程使用延迟任务提交
        if (consumeAfterSend) {
            testTime += caseInterval;
            EventLoop eventLoop = new DefaultEventLoop();
            eventLoop.schedule(() -> {
                // 先关闭生产者
                isStop.set(true);
                try {
                    Thread.sleep(caseInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // 打开消费者
                isStop.set(false);
                // 提交消费任务
                for (int i = 0; i < consumerNum; i++) {
                    perfTestExecutorService.execute(() -> startConsume(syncAck));
                }
            }, testTimePerCase / 2, TimeUnit.MINUTES);
        } else {
            for (int i = 0; i < consumerNum; i++) {
                perfTestExecutorService.execute(() -> startConsume(syncAck));
            }
        }
        // 测试10分钟后停止
        try {
            Thread.sleep(testTime);
        } catch (InterruptedException e) {
            log.error("测试线程sleep异常", e);
        }

        isStop.set(true);

        // 停止2分钟待生产者和消费者关闭
        try {
            Thread.sleep(closeTime);
        } catch (InterruptedException e) {
            log.error("测试线程sleep异常", e);
        }
    }

    private void syncSend(int threadNum, int msgSize) {

        Producer<byte[]> producer = createProducer();
        if (producer == null) {
            return;
        }
        if (threadNum == 1) {
            // 单线程发送消息
            while (!isStop.get()) {
                try {
                    producer.send(new byte[msgSize]);
                    messagesSent.increment();
                    bytesSent.add(msgSize);
                } catch (TlqcnClientException e) {
                    log.error("发送消息异常", e);
                }
            }
        } else {
            // 多线程发送消息
            // 启动生产者线程发送消息
            for (int i = 0; i < threadNum; i++) {
                perfTestExecutorService.execute(() -> {
                    while (!isStop.get()) {
                        try {
                            producer.sendAsync(new byte[msgSize]).get();
                            messagesSent.increment();
                            bytesSent.add(msgSize);
                        } catch (ExecutionException | InterruptedException e) {
                            log.error("发送消息异常", e);
                        }
                    }
                });
            }
        }
        while (!isStop.get()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                log.error("测试线程sleep异常", e);
            }
        }
        try {
            producer.close();
        } catch (TlqcnClientException e) {
            log.error("关闭producer异常", e);
        }
    }

    private void asyncSend(int threadNum, int msgSize) {

        Producer<byte[]> producer = createProducer();
        if (producer == null) {
            return;
        }
        byte[] bytes = new byte[msgSize];
        // 启动生产者线程发送消息
        for (int i = 0; i < threadNum; i++) {
            perfTestExecutorService.execute(() -> {
                while (!isStop.get()) {
                    producer.sendAsync(bytes)
                            .thenAccept(messageId -> {
                               messagesSent.increment();
                               bytesSent.add(msgSize);
                            });
                }
            });
        }
        while (!isStop.get()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                log.error("测试线程sleep异常", e);
            }
        }
        try {
            producer.close();
        } catch (TlqcnClientException e) {
            log.error("关闭producer异常", e);
        }
    }

    private void startConsume(boolean syncAck) {
        try {
            Consumer<byte[]> consumer = tlqcnClient.newConsumer(Schema.BYTES)
                    .topic(topic)
                    .subscriptionName("perf_test_subscription")
                    .subscriptionType(SubscriptionType.Shared)
                    .receiverQueueSize(100000)
                    .autoScaledReceiverQueueSizeEnabled(true)
                    .messageListener((consumer1, msg) -> {
                        messagesReceived.increment();
                        bytesReceived.add(msg.size());
                        if (syncAck) {
                            try {
                                consumer1.acknowledge(msg);
                            } catch (TlqcnClientException e) {
                                log.error("ack error", e);
                            }
                        } else {
                            consumer1.acknowledgeAsync(msg);
                        }
                    })
                    .subscribe();

            while (!isStop.get()) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    log.error("测试线程sleep异常", e);
                }
            }
            try {
                consumer.close();
            } catch (TlqcnClientException e) {
                log.error("关闭pconsumer异常", e);
            }
        } catch (TlqcnClientException e) {
            log.error("创建consumer异常", e);
        }
    }

}
