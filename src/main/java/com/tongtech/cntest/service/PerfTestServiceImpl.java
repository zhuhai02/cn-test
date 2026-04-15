package com.tongtech.cntest.service;

import com.tongtech.cntest.service.api.AdminService;
import com.tongtech.tlqcn.client.api.*;
import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.PerfTestService;
import com.tongtech.cntest.utils.PaddingDecimalFormat;
import java.text.DecimalFormat;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    private final AtomicLong sendMsgCount = new AtomicLong(0);
    private final AtomicLong receiveMsgCount = new AtomicLong(0);
    private final AtomicBoolean isTesting = new AtomicBoolean(false);


    private final ExecutorService perfTestExecutorService;
    private final TlqcnClient tlqcnClient;
    private final AdminService adminService;

    @Autowired
    public PerfTestServiceImpl(TlqcnClient tlqcnClient, AdminService adminService,
                               ExecutorService perfTestExecutorService,
                               TlqcnProperties tlqcnProperties) {
        this.tlqcnClient = tlqcnClient;
        this.adminService = adminService;
        this.perfTestExecutorService = perfTestExecutorService;
    }



    private Producer<byte[]> createProducer(String topic) {
        try {
            return tlqcnClient.newProducer(Schema.BYTES)
                    .topic(topic)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(10000)
                    .batchingMaxBytes(5 * 1024 * 1024)
                    .blockIfQueueFull(true)
                    .create();
        } catch (TlqcnClientException e) {
            log.error("创建producer异常", e);
        }
        return null;
    }


    public String startTest(String topic, int topicPartitionNum, int msgSize,
                          boolean isReCreateTopic, boolean isSyncSend,
                          int producerNum, int consumerNum, int testMinutes) {
        if (isTesting.compareAndSet(false, true)) {
            return "已有性能测试任务在进行，不支持多任务同时执行";
        }
        if (producerNum < 1 && consumerNum < 1) {
            return "生产者和消费者数量不能全部为0";
        }
        if (isReCreateTopic) {
            if (!adminService.clearAndCreatePartitionTopic(topic, topicPartitionNum)) {
                return "主题重建失败";
            }
        }
        sendMsgCount.set(0);
        receiveMsgCount.set(0);
        String testNum = UUID.randomUUID().toString();
        producerLog.info(PERF_PRODUCER_TEST, "==============性能测试开始，测试编号[{}]==============", testNum);
        consumerLog.info(PERF_PRODUCER_TEST, "==============性能测试开始，测试编号[{}]==============", testNum);
        producerLog.info(PERF_PRODUCER_TEST, "主题[{}] 分区数[{}] 消息大小[{}] 测试时长[{}分钟]"
                        + " 生产者数量[{}] 消费者数量[{}] 是否同步发送{}] 是否重建主题[{}]",
                topic, topicPartitionNum, msgSize, testMinutes, producerNum, consumerNum, isSyncSend, isReCreateTopic);
        consumerLog.info(PERF_PRODUCER_TEST, "主题[{}] 分区数[{}] 消息大小[{}] 测试时长[{}分钟]"
                        + " 生产者数量[{}] 消费者数量[{}] 是否同步发送{}] 是否重建主题[{}]",
                topic, topicPartitionNum, msgSize, testMinutes, producerNum, consumerNum, isSyncSend, isReCreateTopic);
        long endTime = System.currentTimeMillis() + (long) testMinutes * 60 * 1000;
        recordMetric(endTime, testNum);
        startConsumer(topic, consumerNum, endTime);
        startProducer(topic, msgSize, isSyncSend, producerNum, endTime);
        return "测试任务已经提交，请测试完毕后查看logs/perf_consumer/producer_test.log中测试编号[" + testNum + "]之间的日志,"
                + "如果集群安装了指标监控组件，可在grafana查看性能数据";
    }

    private void startProducer(String topic, int msgSize, boolean isSyncSend, int producerNum, long endTime) {

        byte[] bytes = new byte[msgSize];
        for (int i = 0; i < producerNum; i++) {
            perfTestExecutorService.execute(() -> {
                Producer<byte[]> producer = createProducer(topic);
                if (producer == null) {
                    return;
                }
                while (endTime > System.currentTimeMillis()) {
                    if (isSyncSend) {
                        try {
                            producer.send(bytes);
                            messagesSent.increment();
                            bytesSent.add(msgSize);
                            sendMsgCount.incrementAndGet();
                        } catch (TlqcnClientException e) {
                            log.error("发送消息异常", e);
                        }
                    } else {
                        producer.sendAsync(bytes)
                                .thenAccept(messageId -> {
                                    messagesSent.increment();
                                    bytesSent.add(msgSize);
                                    sendMsgCount.incrementAndGet();
                                });
                    }
                }
                try {
                    producer.close();
                } catch (TlqcnClientException e) {
                    log.error("关闭producer异常", e);
                }
            });
        }
    }


    private void  recordMetric(long endTime, String testNum) {
        perfTestExecutorService.execute(() -> {
            long oldTime = System.nanoTime();
            while (System.currentTimeMillis() < endTime) {
                try {
                    Thread.sleep(10000);
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
                        "生产指标: {} msg/s --- {} MB/s ---{} msg",
                        THROUGHPUTFORMAT.format(sentRate), THROUGHPUTFORMAT.format(sentThroughput), sendMsgCount.get());

                consumerLog.info(PERF_CONSUMER_TEST,
                        "消费指标: {}  msg/s --- {} MB/s --- {}msg ",
                        THROUGHPUTFORMAT.format(receivedRate), THROUGHPUTFORMAT.format(receivedThroughput), receiveMsgCount.get());

                oldTime = now;
            }
            producerLog.info(PERF_PRODUCER_TEST, "==============性能测试结束，测试编号[{}]==============", testNum);
            consumerLog.info(PERF_PRODUCER_TEST, "==============性能测试结束，测试编号[{}]==============", testNum);
            isTesting.compareAndSet(true, false);
        });
    }

    private void startConsumer(String topic, int consumerNum, long endTime) {
        for (int i = 0; i < consumerNum; i++) {
            perfTestExecutorService.submit(() -> {
                Consumer<byte[]> consumer = null;
                try {
                    consumer = tlqcnClient.newConsumer(Schema.BYTES)
                            .topic(topic)
                            .subscriptionName("perf_test_subscription")
                            .subscriptionType(SubscriptionType.Shared)
                            .receiverQueueSize(100000)
                            .autoScaledReceiverQueueSizeEnabled(true)
                            .subscribe();
                    while (endTime > System.currentTimeMillis()) {
                        Message<byte[]> msg = consumer.receive();
                        messagesReceived.increment();
                        bytesReceived.add(msg.size());
                        consumer.acknowledge(msg);
                        receiveMsgCount.incrementAndGet();
                    }
                } catch (TlqcnClientException e) {
                    log.error("消费流程出现异常", e);
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (TlqcnClientException e) {
                            log.error("消费者关闭异常", e);
                        }
                    }
                }
            });
        }
    }

}
