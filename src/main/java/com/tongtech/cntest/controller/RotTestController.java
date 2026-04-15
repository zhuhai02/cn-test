package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.RotTestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/rot-test")
@Tag(name = "RocketMQ over TongLINK/Q 测试接口", description = "RocketMQ over TongLINK/Q 协议测试接口")
public class RotTestController {

    private final RotTestService rotTestService;

    @Autowired
    public RotTestController(RotTestService rotTestService) {
        this.rotTestService = rotTestService;
    }

    @PostMapping("/sync-send")
    @Operation(summary = "同步发送消息测试",
               description = "测试 RocketMQ over TongLINK/Q 协议的同步发送消息功能。测试结果会记录在 logs/rot_test.log 文件中")
    public String syncSendTest(
            @Parameter(description = "生产者组名称，默认 rot-producer-group", example = "rot-producer-group")
            @RequestParam(defaultValue = "rot-producer-group", required = false) String producerGroup,
            @Parameter(description = "测试的主题名称，默认 rot-sync-send-test", example = "rot-sync-send-test")
            @RequestParam(defaultValue = "rot-sync-send-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return rotTestService.syncSendTest(producerGroup, topic, msgNum);
    }

    @PostMapping("/async-send")
    @Operation(summary = "异步发送消息测试",
               description = "测试 RocketMQ over TongLINK/Q 协议的异步发送消息功能。测试结果会记录在 logs/rot_test.log 文件中")
    public String asyncSendTest(
            @Parameter(description = "生产者组名称，默认 rot-producer-group", example = "rot-producer-group")
            @RequestParam(defaultValue = "rot-producer-group", required = false) String producerGroup,
            @Parameter(description = "测试的主题名称，默认 rot-async-send-test", example = "rot-async-send-test")
            @RequestParam(defaultValue = "rot-async-send-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return rotTestService.asyncSendTest(producerGroup, topic, msgNum);
    }

    @PostMapping("/consumer-pull")
    @Operation(summary = "消费者拉取消息测试",
               description = "测试 RocketMQ over TongLINK/Q 协议的消费者拉取消息功能。测试结果会记录在 logs/rot_test.log 文件中")
    public String consumerPullTest(
            @Parameter(description = "测试的主题名称，默认 rot-consumer-pull-test", example = "rot-consumer-pull-test")
            @RequestParam(defaultValue = "rot-consumer-pull-test", required = false) String topic,
            @Parameter(description = "消费者组名称，默认 rot-consumer-group", example = "rot-consumer-group")
            @RequestParam(defaultValue = "rot-consumer-group", required = false) String consumerGroup) {
        return rotTestService.consumerPullTest(topic, consumerGroup);
    }

    @PostMapping("/consumer-push")
    @Operation(summary = "消费者推送消息测试",
               description = "测试 RocketMQ over TongLINK/Q 协议的消费者推送消息功能。测试结果会记录在 logs/rot_test.log 文件中")
    public String consumerPushTest(
            @Parameter(description = "测试的主题名称，默认 rot-consumer-push-test", example = "rot-consumer-push-test")
            @RequestParam(defaultValue = "rot-consumer-push-test", required = false) String topic,
            @Parameter(description = "消费者组名称，默认 rot-consumer-group", example = "rot-consumer-group")
            @RequestParam(defaultValue = "rot-consumer-group", required = false) String consumerGroup,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return rotTestService.consumerPushTest(topic, consumerGroup, msgNum);
    }
}
