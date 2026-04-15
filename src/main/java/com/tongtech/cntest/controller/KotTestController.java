package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.KotTestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kot-test")
@Tag(name = "Kafka over TongLINK/Q 测试接口", description = "Kafka over TongLINK/Q 协议测试接口")
public class KotTestController {

    private final KotTestService kotTestService;

    @Autowired
    public KotTestController(KotTestService kotTestService) {
        this.kotTestService = kotTestService;
    }

    @PostMapping("/sync-send")
    @Operation(summary = "同步发送消息测试",
               description = "测试 Kafka over TongLINK/Q 协议的同步发送消息功能。测试结果会记录在 logs/kot_test.log 文件中")
    public String syncSendTest(
            @Parameter(description = "测试的主题名称，默认 kot-sync-send-test", example = "kot-sync-send-test")
            @RequestParam(defaultValue = "kot-sync-send-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return kotTestService.syncSendTest(topic, msgNum);
    }

    @PostMapping("/simple-consumer")
    @Operation(summary = "简单消费者测试",
               description = "测试 Kafka over TongLINK/Q 协议的简单消费者功能。测试结果会记录在 logs/kot_test.log 文件中")
    public String simpleConsumerTest(
            @Parameter(description = "测试的主题名称，默认 kot-simple-consumer-test", example = "kot-simple-consumer-test")
            @RequestParam(defaultValue = "kot-simple-consumer-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return kotTestService.simpleConsumerTest(topic, msgNum);
    }
}
