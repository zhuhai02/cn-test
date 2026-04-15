package com.tongtech.cntest.config;


import com.tongtech.tlqcn.client.admin.TlqcnAdmin;
import com.tongtech.tlqcn.client.api.AuthenticationFactory;
import com.tongtech.tlqcn.client.api.ClientBuilder;
import com.tongtech.tlqcn.client.api.SizeUnit;
import com.tongtech.tlqcn.client.api.TlqcnClient;
import com.tongtech.tlqcn.client.api.TlqcnClientException;
import com.tongtech.tlqcn.shade.io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class TlqcnClientConfig {

    private final TlqcnProperties tlqcnProperties;



    @Bean
    public TlqcnAdmin tlqcnAdmin() throws TlqcnClientException {
        return TlqcnAdmin.builder()
                .serviceHttpUrl(tlqcnProperties.getClient().getServiceHttpUrl())
                .build();

    }
    /**
     * 单例类型TlqcnClient，适用于生产者的创建，和使用receive接口消费的消费者使用
     * @return
     * @throws TlqcnClientException
     */
    @Bean
    public TlqcnClient tlqcnClient() throws TlqcnClientException {
        if (tlqcnProperties.getClient() == null || tlqcnProperties.getClient().getServiceUrl() == null) {
            throw new IllegalStateException("TlqcnClient configuration is missing. Please configure tlqcn.client.service-url in application.yml");
        }
        ClientBuilder builder = TlqcnClient.builder()
                // 配置TongLINK/Q-CN服务器地址
                .serviceUrl(tlqcnProperties.getClient().getServiceUrl());

        return builder.build();
    }

    @Bean
    public ExecutorService perfTestExecutorService() {
        return Executors.newCachedThreadPool(new DefaultThreadFactory("tlqcn-perf-test"));
    }

}