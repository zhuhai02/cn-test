package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.AotTestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/aot-test")
@Tag(name = "AMQP over TongLINK/Q 测试接口", description = "AMQP over TongLINK/Q 协议测试接口")
public class AotTestController {

    private final AotTestService aotTestService;

    @Autowired
    public AotTestController(AotTestService aotTestService) {
        this.aotTestService = aotTestService;
    }

    @PostMapping("/default-exchange")
    @Operation(summary = "默认交换机测试",
               description = "测试 AMQP over TongLINK/Q 协议的默认交换机功能。测试结果会记录在 logs/aot_test.log 文件中")
    public String defaulExchangeTest(
            @Parameter(description = "默认队列名称，默认 aot-default-queue", example = "aot-default-queue")
            @RequestParam(defaultValue = "aot-default-queue", required = false) String defaultQueue,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return aotTestService.defaulExchangeTest(defaultQueue, msgNum);
    }

    @PostMapping("/direct-exchange")
    @Operation(summary = "直连交换机测试",
               description = "测试 AMQP over TongLINK/Q 协议的直连交换机（Direct Exchange）功能。测试结果会记录在 logs/aot_test.log 文件中")
    public String directExchangeTest(
            @Parameter(description = "虚拟主机名称，默认 /", example = "/")
            @RequestParam(defaultValue = "/", required = false) String virtualHost,
            @Parameter(description = "队列名称，默认 aot-direct-queue", example = "aot-direct-queue")
            @RequestParam(defaultValue = "aot-direct-queue", required = false) String queueName,
            @Parameter(description = "交换机名称，默认 aot-direct-exchange", example = "aot-direct-exchange")
            @RequestParam(defaultValue = "aot-direct-exchange", required = false) String exchangeName,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return aotTestService.directExchangeTest(virtualHost, queueName, exchangeName, msgNum);
    }

    @PostMapping("/fanout-exchange")
    @Operation(summary = "扇出交换机测试",
               description = "测试 AMQP over TongLINK/Q 协议的扇出交换机（Fanout Exchange）功能。测试结果会记录在 logs/aot_test.log 文件中")
    public void fanoutExchangeTest() {
        aotTestService.fanoutExchangeTest();
    }

    @PostMapping("/topic-exchange")
    @Operation(summary = "主题交换机测试",
               description = "测试 AMQP over TongLINK/Q 协议的主题交换机（Topic Exchange）功能。测试结果会记录在 logs/aot_test.log 文件中")
    public void topicExchangeTest() {
        aotTestService.topicExchangeTest();
    }
}
