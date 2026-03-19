package com.tongtech.cntest.start;

import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProtocolTestStart {
    private final RotTestService rotTestService;
    private final AotTestService aotTestService;
    private final KotTestService kotTestService;

    private final MotTestService motTestService;

    private final JmsTestService jmsTestService;

    private boolean rotEnabled;
    private boolean aotEnabled;
    private boolean kotEnabled;
    private boolean motEnabled;
    private boolean jmsEnabled;

    @Autowired
    public ProtocolTestStart(RotTestService rotTestService,
                             AotTestService aotTestService,
                             KotTestService kotTestService,
                             MotTestService motTestService,
                             JmsTestService jmsTestService,
                             TlqcnProperties tlqcnProperties) {
        this.rotTestService = rotTestService;
        this.aotTestService = aotTestService;
        this.kotTestService = kotTestService;
        this.motTestService = motTestService;
        this.jmsTestService = jmsTestService;
        this.rotEnabled = tlqcnProperties.getRotTestConfig().isEnabled();
        this.aotEnabled = tlqcnProperties.getAotTestConfig().isEnabled();
        this.kotEnabled = tlqcnProperties.getKotTestConfig().isEnabled();
        this.motEnabled = tlqcnProperties.getMotTestConfig().isEnabled();
        this.jmsEnabled = tlqcnProperties.getJmsTestConfig().isEnabled();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!rotEnabled) {
            log.info("rocketmq协议支持测试未开启");
        } else {
            log.info("rocketmq协议支持测试开启");
            rotTestService.startTest();
        }

        if (!aotEnabled) {
            log.info("amqp协议支持测试未开启");
        } else {
            log.info("amqp协议支持测试开启");
            aotTestService.startTest();
        }

        if (!kotEnabled) {
            log.info("kafka协议支持测试未开启");
        } else {
            log.info("kafka协议支持测试开启");
            kotTestService.startTest();
        }

        if (!motEnabled) {
            log.info("Mqtt协议支持测试未开启");
        } else {
            log.info("Mqtt协议支持测开启");
            motTestService.startTest();
        }

        if (!jmsEnabled) {
            log.info("Jms协议支持测试未开启");
        } else {
            log.info("Jms协议支持测试开启");
            jmsTestService.startTest();
        }
    }
}
