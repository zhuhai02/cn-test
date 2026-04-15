package com.tongtech.cntest.service;

import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.RotTestService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CountDownLatch;

@Service
public class RotTestServiceImpl implements RotTestService {
    private static final Logger log = LoggerFactory.getLogger("PROTOCOL_TEST_LOGGER");

    private static final Marker PROTOCOL_TEST = MarkerFactory.getMarker("PROTOCOL_TEST");

    private String url;


    @Autowired
    public RotTestServiceImpl(TlqcnProperties tlqcnProperties) {
        this.url = tlqcnProperties.getRotTestConfig().getUrl();
    }

    public DefaultMQProducer createProducer(String producerGroup) {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(url);
        try {
            producer.start();
            return producer;
        } catch (MQClientException e) {
            log.error(PROTOCOL_TEST, "producer 创建失败", e);
        }
        return null;
    }

    private void createTopic(DefaultMQProducer producer, String topic) {
        try {
            for (int i = 0; i < 1; i++) {
                Date date = new Date();

                List<Message> messages = new ArrayList<>();

                String msgStr = "创建topic，num:" + i + ",time:" + date.getTime();

                Message msg = new Message(topic,
                        "tag_create",
                        (msgStr).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );

                messages.add(msg);
                producer.send(messages);
            }
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Rocketmq协议创建topic失败，topic：<{}>", topic, e);
        }
    }

    private void simpleSend(DefaultMQProducer producer, String topic, int size, String tag) {
        try {
            for (int i = 0; i < size; i++) {
                Date date = new Date();

                List<Message> messages = new ArrayList<>();

                String msgStr = "Rocketmq协议简单消息，num:" + i + ",time:" + date.getTime();

                Message msg = new Message(topic,
                        tag,
                        (msgStr).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );

                messages.add(msg);
                SendResult sendResult = producer.send(messages);
                log.info(PROTOCOL_TEST, msgStr + "，sendResult：<{}>", sendResult.getSendStatus());
            }
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Rocketmq协议标准消息发送失败，topic：<{}>", topic, e);
        }
    }

    @Override
    public String syncSendTest(String producerGroup, String topic, int msgNum) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------Rocketmq协议同步发送测试开始,测试编号[{}]----------", testNum);
        DefaultMQProducer producer = createProducer(producerGroup);

        try {
            for (int i = 0; i < msgNum; i++) {
                Date date = new Date();

                List<Message> messages = new ArrayList<>();

                String msgStr = "Rocketmq协议同步发送消息，num:" + i + ",time:" + date.getTime();

                Message msg = new Message(topic,
                        (msgStr).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );

                messages.add(msg);
                SendResult sendResult = producer.send(messages);
                log.info(PROTOCOL_TEST, msgStr + "，sendResult：<{}>", sendResult.getSendStatus());
            }

            log.info(PROTOCOL_TEST, "Rocketmq协议同步发送测试完成");
            producer.shutdown();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Rocketmq协议同步发送测试失败，topic：<{}>", topic, e);
        }
        log.info(PROTOCOL_TEST, "----------Rocketmq协议同步发送测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }

    @Override
    public String asyncSendTest(String producerGroup, String topic, int msgNum) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------Rocketmq协议异步发送测试开始,测试编号[{}]----------", testNum);
        DefaultMQProducer producer = createProducer(producerGroup);

        try {
            producer.setRetryTimesWhenSendAsyncFailed(0);

            final CountDownLatch countDownLatch = new CountDownLatch(msgNum);

            for (int i = 0; i < msgNum; i++) {
                try {
                    Date date = new Date();

                    String msgStr = "Rocketmq协议同步发送消息，num:" + i + ",time:" + date.getTime();

                    Message msg = new Message(topic,
                            msgStr.getBytes(RemotingHelper.DEFAULT_CHARSET));

                    // 异步发送消息, 发送结果通过callback返回给客户端
                    producer.send(msg, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            log.info(PROTOCOL_TEST, "发送成功：<{}>", msgStr);
                            countDownLatch.countDown();
                        }

                        @Override
                        public void onException(Throwable e) {
                            log.error(PROTOCOL_TEST, "Rocketmq协议异步发送测试失败，topic：<{}>", topic, e);
                            countDownLatch.countDown();
                        }
                    });
                } catch (Exception e) {
                    log.error(PROTOCOL_TEST, "Rocketmq协议异步发送测试失败，topic：<{}>", topic, e);
                    countDownLatch.countDown();
                }
            }

            //异步发送，如果要求可靠传输，必须要等回调接口返回明确结果后才能结束逻辑，否则立即关闭Producer可能导致部分消息尚未传输成功
            countDownLatch.await();
            log.info(PROTOCOL_TEST, "Rocketmq协议异步发送测试完成");
            producer.shutdown();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Rocketmq协议异步发送测试失败，topic：<{}>", topic, e);
        }
        log.info(PROTOCOL_TEST, "----------Rocketmq协议异步发送测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }

    @Override
    public String consumerPullTest(String topic, String consumerGroup) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------Rocketmq协议pull消费测试开始,测试编号[{}]----------", testNum);
        DefaultMQProducer producer = createProducer("consumerPullTest");
        try {
            createTopic(producer, topic);

            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
            consumer.setNamesrvAddr(url);
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.setInstanceName("ConsumerInstance_01");
            consumer.start();

            simpleSend(producer,topic,16, "tag_pull");

            Map<MessageQueue, Long> offsetTable = new HashMap<>();

            int i = 0;

            while (i < 20) {
                i++;

                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
                // 4. 遍历所有队列
                for (MessageQueue mq : mqs) {
                    try {
                        // 5. 初始化偏移量
                        long offset = offsetTable.get(mq) != null ? offsetTable.get(mq) : -1;
                        if (offset < 0) {
                            offset = consumer.maxOffset(mq);
                        }

                        // 7. 核心拉取方法
                        PullResult pullResult = consumer.pullBlockIfNotFound(
                                mq,
                                "tag_pull",
                                offset,
                                32
                        );

                        List<MessageExt> messages = pullResult.getMsgFoundList();

                        if (messages != null && messages.size() > 0) {
                            for (MessageExt message : messages) {
                                try {
                                    log.info(PROTOCOL_TEST, "Rocketmq协议pull消费成功：<{}>", new String(message.getBody()));
                                    consumer.updateConsumeOffset(mq, message.getQueueOffset() + 1);
                                } catch (Exception e) {

                                }
                            }
                        } else {
                        }

                        offset = pullResult.getNextBeginOffset();
                        offsetTable.put(mq, offset);

                        switch (pullResult.getPullStatus()) {
                            case FOUND:     // 继续拉取下一条
                                break;
                            case NO_NEW_MSG: // 没有新消息
                            case NO_MATCHED_MSG:
                                Thread.sleep(1000); // 暂停1秒
                                break;
                            case OFFSET_ILLEGAL: // 偏移量非法
                                offset = consumer.minOffset(mq);
                                break;
                            default:
                                break;
                        }
                    } catch (Exception e) {
                        log.error(PROTOCOL_TEST, "Rocketmq协议pull消费测试失败，topic：<{}>", topic, e);
                    }
                }

                Thread.sleep(1000 * 3);
            }

            log.info(PROTOCOL_TEST, "Rocketmq协议pull消费测试完成");
            producer.shutdown();
            consumer.shutdown();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Rocketmq协议pull消费测试失败，topic：<{}>", topic, e);
        }
        log.info(PROTOCOL_TEST, "----------Rocketmq协议pull消费测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }

    @Override
    public String consumerPushTest(String topic, String consumerGroup, int msgNum) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------Rocketmq协议push消费测试开始,测试编号[{}]----------", testNum);
        DefaultMQProducer producer = createProducer("consumerPushTest");
        try {
            createTopic(producer,  topic);

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(url);

            consumer.subscribe(topic, "tag_push");
            consumer.setMessageModel(MessageModel.CLUSTERING);

            consumer.registerMessageListener(new MessageListenerConcurrently() {
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    for (MessageExt ext : msgs) {
                        log.info(PROTOCOL_TEST, "Rocketmq协议push消费成功：<{}>", new String(ext.getBody()));
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();

            simpleSend(producer, topic, msgNum, "tag_push");

            Thread.sleep(1000 * 30);
            log.info(PROTOCOL_TEST, "Rocketmq协议push消费测试完成");
            producer.shutdown();
            consumer.shutdown();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "Rocketmq协议push消费测试失败，topic：<{}>", topic, e);
        }
        log.info(PROTOCOL_TEST, "----------Rocketmq协议push消费测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }
}
