package com.tongtech.cntest.utils;

import com.tongtech.tlqcn.client.api.BatchReceivePolicy;
import com.tongtech.tlqcn.client.api.TlqcnClient;
import com.tongtech.tlqcn.client.api.TlqcnClientException;
import com.tongtech.tlqcn.client.api.Consumer;
import com.tongtech.tlqcn.client.api.Messages;
import com.tongtech.tlqcn.client.api.Producer;
import com.tongtech.tlqcn.client.api.Schema;
import com.tongtech.tlqcn.client.api.SubscriptionInitialPosition;
import com.tongtech.tlqcn.client.api.SubscriptionType;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PriorityUtil {

    private static final String subName = "sub";

    private static TlqcnClient client;

    private static Map<Integer, Producer<byte[]>> priorityProducerMap = new ConcurrentHashMap<>();

    private static Map<Integer, Consumer<byte[]>> priorityConsumerMap = new ConcurrentHashMap<>();

    private static Map<Integer, Messages<byte[]>> priorityMessagesMap = new ConcurrentHashMap<>();


    public static void startTest(int totalPriority, String topicPrefix, String serviceUrl, boolean isAbsolutePriority)
            throws ExecutionException, InterruptedException, TlqcnClientException {
        init(serviceUrl, topicPrefix, totalPriority);
        log.info("------------------消息优先级测试开始-------------------------");
        int messageNum = 20;
        for (int i = 0; i < messageNum; i++) {
            int priority = i % totalPriority;
            String message = MessageFormat.format("优先级<{0}> : 消息顺序<{1}>", priority, i);
            send(priority, (message).getBytes());
            log.info("消息发送成功<{}>", message);
        }
        log.info("++++++++++++消息发送完毕++++++++++++++");
        while (messageNum > 0) {
            receive(totalPriority);
            for (int i = 0; i < totalPriority; i++) {
                Messages<byte[]> messages = priorityMessagesMap.get(i);
                if (messages != null) {
                    messages.forEach(message -> {
                       log.info("消费者消费到消息<{}>", new String(message.getValue()));
                    });
                    priorityConsumerMap.get(i).acknowledgeAsync(messages);
                    messageNum -= messages.size();
                    priorityMessagesMap.remove(i);
//                 如果要让高优先级的消息被消费完，才消费低优先级的消息，可以加上如下逻辑
                    if (isAbsolutePriority) {
                        if (messages.size() != 0) {
                            break;
                        }
                    }
                }
            }
        }
        log.info("------------------消息优先级测试结束-------------------------");

        client.close();
    }

    private static void init(String serviceUrl, String topicPrefix, int totalPriority) throws ExecutionException, InterruptedException, TlqcnClientException {
        client = TlqcnClient.builder()
                .serviceUrl(serviceUrl)
                .build();
        // 初始化对应优先级的生产者
        Map<Integer, CompletableFuture<Producer<byte[]>>> producerCompletableFutureMap = new HashMap<>();
        for (int i = 0; i < totalPriority; i++) {
            producerCompletableFutureMap.put(i, client.newProducer(Schema.BYTES)
                   .topic(topicPrefix + i)
                   .createAsync());
        }
        CompletableFuture.allOf(producerCompletableFutureMap.values().toArray(new CompletableFuture[0])).get();
        for (int i = 0; i < totalPriority; i++) {
            priorityProducerMap.put(i, producerCompletableFutureMap.get(i).get());
        }
        // 初始化对应优先级的消费者
        Map<Integer, CompletableFuture<Consumer<byte[]>>> consumerCompletableFutureMap = new HashMap<>();
        for (int i = 0; i < totalPriority; i++) {
            consumerCompletableFutureMap.put(i,
                    client.newConsumer(Schema.BYTES)
                            .topic(topicPrefix + i)
                            .subscriptionName(subName)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscriptionType(SubscriptionType.Shared)
                            // 设置单次拉取消息的最大数量，高优先级尽量获取更多的消息数量
                            .receiverQueueSize((totalPriority - i))
                            .batchReceivePolicy(BatchReceivePolicy.builder()
                                    .timeout(100, TimeUnit.MILLISECONDS)
                                    .maxNumMessages((totalPriority - i))
                                    .build())
                            .subscribeAsync()
            );
        }
        CompletableFuture.allOf(consumerCompletableFutureMap.values().toArray(new CompletableFuture[0])).get();
        for (int i = 0; i < totalPriority; i++) {
            priorityConsumerMap.put(i, consumerCompletableFutureMap.get(i).get());
        }
//        receiveTask();
    }

    private static void send(int priority, byte[] bytes) throws TlqcnClientException {
        priorityProducerMap.get(priority).send(bytes);
    }

    /**
     * 异步轮询获取各优先级队列内的消息
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static void receive(int totalPriority) throws ExecutionException, InterruptedException {
        Map<Integer, CompletableFuture<Messages<byte[]>> > priorityCompletableFuture = new HashMap<>();
        for (int i = 0; i < totalPriority; i++) {
            Messages<byte[]> messages = priorityMessagesMap.get(i);
            if (messages != null && messages.size() != 0) {
                continue;
            }
            priorityCompletableFuture.put(i, priorityConsumerMap.get(i).batchReceiveAsync());
        }
        CompletableFuture.allOf(priorityCompletableFuture.values().toArray(new CompletableFuture[0])).get();
        for (int i = 0; i < totalPriority; i++) {
            Messages<byte[]> messages = priorityMessagesMap.get(i);
            if (messages != null && messages.size() != 0) {
                continue;
            }
            priorityMessagesMap.put(i, priorityCompletableFuture.get(i).get());
        }
    }
}
