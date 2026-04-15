package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.FunctionTestService;
import com.tongtech.tlqcn.client.api.SubscriptionType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/function-test")
@Tag(name = "功能测试接口", description = "TongLINK/Q-CN 消息队列功能测试接口，用于测试各种消息队列特性")
public class FunctionTestController {

    private final FunctionTestService functionTestService;

    @Autowired
    public FunctionTestController(FunctionTestService functionTestService) {
        this.functionTestService = functionTestService;
    }

    @PostMapping("/sync-send")
    @Operation(summary = "同步发送消息测试",
               description = "测试同步发送消息功能，按顺序同步发送指定数量的消息，前一条消息发送完毕才会进行下一条消息的发送。测试结果会记录在 logs/function_test.log 文件中")
    public String syncSendTest(
            @Parameter(description = "测试的主题名称，默认 sync-send-test", example = "sync-send-test")
            @RequestParam(defaultValue = "sync-send-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) Integer msgNum) {
        return functionTestService.syncSendTest(topic, msgNum);
    }

    @PostMapping("/async-send")
    @Operation(summary = "异步发送消息测试",
               description = "测试异步发送消息功能，按顺序异步发送指定数量的消息，不等待前一条消息发送完成即可发送下一条。测试结果会记录在 logs/function_test.log 文件中")
    public String asyncSendTest(
            @Parameter(description = "测试的主题名称，默认 async-send-test", example = "async-send-test")
            @RequestParam(defaultValue = "async-send-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) Integer msgNum) {
        return functionTestService.asyncSendTest(topic, msgNum);
    }

    @PostMapping("/subscribe-type")
    @Operation(summary = "订阅类型测试",
               description = "测试不同的订阅类型（Exclusive独占、Shared共享、Failover故障转移、Key_Shared按键共享）。" +
                       "不同订阅类型决定了消息如何分发给多个消费者。测试结果会记录在 logs/function_test.log 文件中")
    public String subscribeTypeTest(
            @Parameter(description = "测试的主题名称，默认 subscribe-type-test", example = "subscribe-type-test")
            @RequestParam(defaultValue = "subscribe-type-test", required = false) String topic,
            @Parameter(description = "测试的消息数量（最少10条），默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) Integer msgNum,
            @Parameter(description = "消费者数量")
            @RequestParam(defaultValue = "3", required = false) Integer consumerNum,
            @Parameter(description = "订阅类型：Exclusive(独占)、Shared(共享)、Failover(故障转移)、Key_Shared(按键共享)",
                       required = true, example = "Shared")
            @RequestParam SubscriptionType subscriptionType) {
        return functionTestService.subscribeTypeTest(topic, msgNum, consumerNum, subscriptionType);
    }

    @PostMapping("/message-filter")
    @Operation(summary = "消息过滤测试",
               description = "测试消息过滤功能，包括标签过滤（Tag Filter）和 SQL92 表达式过滤。" +
                       "消费者可以根据消息属性进行过滤，只接收符合条件的消息。测试结果会记录在 logs/function_test.log 文件中")
    public String messageFilterTest(
            @Parameter(description = "测试的主题名称，默认 message-filter-test", example = "message-filter-test")
            @RequestParam(defaultValue = "message-filter-test", required = false) String topic) {
        return functionTestService.messageFilterTest(topic);
    }

    @PostMapping("/message-seek")
    @Operation(summary = "消息回溯测试",
               description = "测试消息回溯功能，可以将消费位置重置到指定的消息ID，重新消费历史消息。" +
                       "测试会发送10条消息，然后将游标重置到第5条消息位置。测试结果会记录在 logs/function_test.log 文件中")
    public String messageSeekTest(
            @Parameter(description = "测试的主题名称，默认 message-seek-test", example = "message-seek-test")
            @RequestParam(defaultValue = "message-seek-test", required = false) String topic) {
        return functionTestService.messageSeekTest(topic);
    }

    @PostMapping("/broadcast-consume")
    @Operation(summary = "广播消费测试",
               description = "测试广播消费功能，使用 Reader 模式，每个 Reader 都能接收到所有消息。" +
                       "测试会创建3个 Reader，每个都会收到全部10条消息。测试结果会记录在 logs/function_test.log 文件中")
    public String broadcastConsumeTest(
            @Parameter(description = "测试的主题名称，默认 broadcast-consume-test", example = "broadcast-consume-test")
            @RequestParam(defaultValue = "broadcast-consume-test", required = false) String topic) {
        return functionTestService.broadcastConsumeTest(topic);
    }

    @PostMapping("/dead-letter-queue")
    @Operation(summary = "死信队列测试",
               description = "测试死信队列功能，当消息重试次数超过最大重试次数后，消息会被发送到死信队列。" +
                       "测试会创建一个消费者持续不确认消息，触发重试机制，最终消息进入死信队列。测试时长约1分钟。测试结果会记录在 logs/function_test.log 文件中")
    public String deadLetterQueueTest(
            @Parameter(description = "测试的主题名称（会自动创建对应的死信主题 topic_DLQ），默认 dead-letter-queue-test", example = "dead-letter-queue-test")
            @RequestParam(defaultValue = "dead-letter-queue-test", required = false) String topic) {
        return functionTestService.deadLetterQueueTest(topic);
    }

    @PostMapping("/consumer-retry")
    @Operation(summary = "消费者重试测试",
               description = "消费者重试测试功能已包含在死信队列测试中，死信队列测试包含了完整的消息重试机制验证")
    public String consumerRetryTest(
            @Parameter(description = "测试的主题名称，默认 consumer-retry-test", example = "consumer-retry-test")
            @RequestParam(defaultValue = "consumer-retry-test", required = false) String topic) {
        return functionTestService.consumerRetryTest(topic);
    }

    @PostMapping("/delay-message")
    @Operation(summary = "延时消息测试",
               description = "测试延时消息功能，消息发送后不会立即被消费，而是在指定的延时时间后才能被消费。" +
                       "测试会发送一条延时消息，并等待延时时间+10秒后结束。测试结果会记录在 logs/function_test.log 文件中")
    public String delayMessageTest(
            @Parameter(description = "测试的主题名称，默认 delay-message-test", example = "delay-message-test")
            @RequestParam(defaultValue = "delay-message-test", required = false) String topic,
            @Parameter(description = "延迟秒数，消息将在指定秒数后才能被消费,默认30s")
            @RequestParam(defaultValue = "30", required = false) Long delaySeconds) {
        return functionTestService.delayMessageTest(topic, delaySeconds);
    }

    @PostMapping("/scheduled-message")
    @Operation(summary = "定时消息测试",
               description = "测试定时消息功能，消息会在指定的时间戳时刻才能被消费。" +
                       "如果指定时间戳在未来，测试会等待到该时刻+2秒后结束。测试结果会记录在 logs/function_test.log 文件中")
    public String scheduledMessageTest(
            @Parameter(description = "测试的主题名称，默认 scheduled-message-test", example = "scheduled-message-test")
            @RequestParam(defaultValue = "scheduled-message-test", required = false) String topic,
            @Parameter(description = "指定消费时间的时间戳（毫秒），消息将在该时刻才能被消费，默认30s后", example = "1712345678000")
            @RequestParam(required = false) Long timestamp) {
        return functionTestService.scheduledMessageTest(topic, timestamp);
    }

    @PostMapping("/message-order")
    @Operation(summary = "消息有序性测试",
               description = "测试消息有序性功能，使用 Exclusive 独占订阅模式确保消息按发送顺序被消费。" +
                       "测试会按顺序发送指定数量的消息，然后验证消费顺序是否与发送顺序一致。测试结果会记录在 logs/function_test.log 文件中")
    public String messageOrderTest(
            @Parameter(description = "测试的主题名称，默认 message-order-test", example = "message-order-test")
            @RequestParam(defaultValue = "message-order-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) Integer msgNum) {
        return functionTestService.messageOrderTest(topic, msgNum);
    }

    @PostMapping("/gm-message")
    @Operation(summary = "国密消息测试",
               description = "测试国密算法加密消息功能，使用国密 SM2/SM3/SM4 算法对消息内容进行加密传输。" +
                       "需要配置国密公钥和私钥路径。测试结果会记录在 logs/function_test.log 文件中")
    public String gmMessageTest(
            @Parameter(description = "测试的主题名称，默认 gm-message-test", example = "gm-message-test")
            @RequestParam(defaultValue = "gm-message-test", required = false) String topic,
            @Parameter(description = "测试的私钥路径，默认 gm_PrivateKey.pem", example = "gm_PrivateKey.pem")
            @RequestParam(defaultValue = "gm_PrivateKey.pem", required = false) String privateKeyPath,
            @Parameter(description = "测试的公钥路径，默认 gm_PublicKey.pem", example = "gm_PublicKey.pem")
            @RequestParam(defaultValue = "gm_PublicKey.pem", required = false) String publicKeyPath) {
        return functionTestService.gmMessageTest(topic, privateKeyPath, publicKeyPath);
    }

    @PostMapping("/gm-tls")
    @Operation(summary = "国密通信测试",
               description = "测试国密 TLS 通信功能，使用国密算法进行传输层加密。" +
                       "注意：该测试暂时无法通过日志观察，需要通过网络抓包等方式验证。测试结果会记录在 logs/function_test.log 文件中")
    public String gmTlsTest(
            @Parameter(description = "测试的主题名称，默认 gm-tls-test", example = "gm-tls-test")
            @RequestParam(defaultValue = "gm-tls-test", required = false) String topic) {
        return functionTestService.gmTlsTest(topic);
    }

    @PostMapping("/message-priority")
    @Operation(summary = "消息优先级测试",
               description = "测试消息优先级功能，支持绝对优先级和相对优先级两种模式。" +
                       "绝对优先级：高优先级消息必须全部消费完才能消费低优先级消息；" +
                       "相对优先级：高优先级消息有更大概率被优先消费，但不保证严格顺序。测试结果会记录在 logs/function_test.log 文件中")
    public String messagePriorityTest(
            @Parameter(description = "测试的主题名称（会为每个优先级创建独立主题，如 topic0, topic1...），默认 message-priority-test", example = "message-priority-test")
            @RequestParam(defaultValue = "message-priority-test", required = false) String topic,
            @Parameter(description = "优先级总数，会创建对应数量的优先级主题,默认3", example = "3")
            @RequestParam(defaultValue = "3", required = false) Integer totalPriority,
            @Parameter(description = "是否使用绝对优先级模式。true=绝对优先级，false=相对优先级", example = "true")
            @RequestParam(defaultValue = "true", required = false) boolean isAbsolutePriority) {
        return functionTestService.messagePriorityTest(topic, totalPriority, isAbsolutePriority);
    }

    @PostMapping("/failover")
    @Operation(summary = "故障转移测试",
               description = "测试集群故障转移功能，当主集群不可用时自动切换到备集群，主集群恢复后自动切回。" +
                       "需要配置主备集群地址。测试会发送指定数量的消息并观察故障转移过程。测试结果会记录在 logs/function_test.log 文件中")
    public String failoverTest(
            @Parameter(description = "测试的主题名称（会在主备集群同时创建），默认 failover-test", example = "failover-test")
            @RequestParam(defaultValue = "failover-test") String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) Integer msgNum) {
        return functionTestService.failoverTest(topic, msgNum);
    }

    @PostMapping("/transaction")
    @Operation(summary = "事务消息测试",
               description = "测试事务消息功能，确保消息的发送和消费在事务中原子性执行。" +
                       "测试场景：从输入主题消费消息，处理后发送到两个输出主题，整个过程在事务中完成，保证要么全部成功要么全部失败。" +
                       "测试结果会记录在 logs/function_test.log 文件中")
    public String transactionTest(
            @Parameter(description = "输入主题名称，用于接收原始消息，默认 transaction-input-test", example = "transaction-input-test")
            @RequestParam(defaultValue = "transaction-input-test") String inputTopic,
            @Parameter(description = "输出主题1名称，用于接收处理后的消息，默认 transaction-output-test-1", example = "transaction-output-test-1")
            @RequestParam(defaultValue = "transaction-output-test-1") String outputTopicOne,
            @Parameter(description = "输出主题2名称，用于接收处理后的消息，默认 transaction-output-test-2", example = "transaction-output-test-2")
            @RequestParam(defaultValue = "transaction-output-test-2") String outputTopicTwo,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) Integer msgNum) {
        return functionTestService.transactionTest(inputTopic, outputTopicOne, outputTopicTwo, msgNum);
    }
}
