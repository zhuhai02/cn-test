package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.MotTestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/mot-test")
@Tag(name = "MQTT over TongLINK/Q 测试接口", description = "MQTT over TongLINK/Q 协议测试接口")
public class MotTestController {

    private final MotTestService motTestService;

    @Autowired
    public MotTestController(MotTestService motTestService) {
        this.motTestService = motTestService;
    }

    @PostMapping("/simple")
    @Operation(summary = "简单测试",
               description = "测试 MQTT over TongLINK/Q 协议的基本发送和接收功能，支持配置 QoS 级别。测试结果会记录在 logs/mot_test.log 文件中")
    public String simpleTest(
            @Parameter(description = "测试的主题名称，默认 mot-simple-test", example = "mot-simple-test")
            @RequestParam(defaultValue = "mot-simple-test", required = false) String topic,
            @Parameter(description = "QoS 级别（0=最多一次，1=至少一次，2=恰好一次），默认1", example = "1")
            @RequestParam(defaultValue = "1", required = false) int qos,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return motTestService.simpleTest(topic, qos, msgNum);
    }
}
