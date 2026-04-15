package com.tongtech.cntest.controller;

import com.tongtech.cntest.service.api.PerfTestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/perf-test")
@Tag(name = "性能测试接口", description = "TongLINK/Q-CN 消息队列性能测试接口，用于测试消息队列的吞吐量和性能指标")
public class PerfTestController {

    private final PerfTestService perfTestService;

    @Autowired
    public PerfTestController(PerfTestService perfTestService) {
        this.perfTestService = perfTestService;
    }

    @PostMapping("/start")
    @Operation(summary = "启动性能测试",
               description = "启动消息队列性能测试，支持配置生产者和消费者数量、消息大小、测试时长等参数。" +
                       "测试会持续指定的时间，并实时输出性能指标（吞吐量、延迟等）。测试结果会记录在 logs/perf_test.log 文件中")
    public String startTest(
            @Parameter(description = "测试的主题名称，默认 perf-test", example = "perf-test")
            @RequestParam(defaultValue = "perf-test", required = false) String topic,
            @Parameter(description = "主题分区数量，分区数影响并发性能，默认4个分区", example = "4")
            @RequestParam(defaultValue = "4", required = false) int topicPartitionNum,
            @Parameter(description = "单条消息大小（字节），默认1024字节（1KB）", example = "1024")
            @RequestParam(defaultValue = "1024", required = false) int msgSize,
            @Parameter(description = "是否重新创建主题，true=删除已存在的主题并重新创建，false=使用已存在的主题", example = "true")
            @RequestParam(defaultValue = "true", required = false) boolean isReCreateTopic,
            @Parameter(description = "是否使用同步发送模式，true=同步发送（等待确认），false=异步发送（不等待确认）", example = "false")
            @RequestParam(defaultValue = "false", required = false) boolean isSyncSend,
            @Parameter(description = "生产者数量，多个生产者可以提高发送吞吐量，默认1个", example = "1")
            @RequestParam(defaultValue = "1", required = false) int producerNum,
            @Parameter(description = "消费者数量，多个消费者可以提高消费吞吐量，默认1个", example = "1")
            @RequestParam(defaultValue = "1", required = false) int consumerNum,
            @Parameter(description = "测试持续时长（分钟），默认5分钟", example = "5")
            @RequestParam(defaultValue = "5", required = false) int testMinutes) {
        return perfTestService.startTest(topic, topicPartitionNum, msgSize,
                isReCreateTopic, isSyncSend, producerNum, consumerNum, testMinutes);
    }
}
