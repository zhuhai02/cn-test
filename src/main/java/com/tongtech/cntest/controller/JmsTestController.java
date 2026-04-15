package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.JmsTestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jms-test")
@Tag(name = "JMS 测试接口", description = "JMS 协议测试接口")
public class JmsTestController {

    private final JmsTestService jmsTestService;

    @Autowired
    public JmsTestController(JmsTestService jmsTestService) {
        this.jmsTestService = jmsTestService;
    }

    @PostMapping("/p2p")
    @Operation(summary = "JMS 点对点测试",
               description = "测试 JMS 协议的点对点（Point-to-Point）消息传递模式。测试结果会记录在 logs/jms_test.log 文件中")
    public String jms2P2PTest(
            @Parameter(description = "测试的主题名称，默认 jms-p2p-test", example = "jms-p2p-test")
            @RequestParam(defaultValue = "jms-p2p-test", required = false) String topic,
            @Parameter(description = "测试的消息数量，默认10条", example = "10")
            @RequestParam(defaultValue = "10", required = false) int msgNum) {
        return jmsTestService.jms2P2PTest(topic, msgNum);
    }
}
