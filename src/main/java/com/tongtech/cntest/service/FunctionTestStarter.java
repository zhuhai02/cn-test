package com.tongtech.cntest.service;

import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.FunctionTestService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class FunctionTestStarter {

    private boolean enabled;

    private final FunctionTestService functionTestService;

    @Autowired
    public FunctionTestStarter(FunctionTestService functionTestService, TlqcnProperties tlqcnProperties) {
        this.functionTestService = functionTestService;
        this.enabled = tlqcnProperties.getFunctionTestConfig().isEnabled();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!enabled) {
            log.info("功能测试未开启");
            return;
        }
        functionTestService.startTest();
    }
}
