package com.tongtech.cntest.config;


import com.tongtech.tlqcn.client.admin.TlqcnAdmin;
import com.tongtech.tlqcn.client.api.AuthenticationFactory;
import com.tongtech.tlqcn.client.api.ClientBuilder;
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
            return null;
        }
        ClientBuilder builder = TlqcnClient.builder()
                // 配置TongLINK/Q-CN服务器地址
                .serviceUrl(tlqcnProperties.getClient().getServiceUrl());

        // 配置认证
        if (tlqcnProperties.getClient().getAuthentication().isEnabled()) {
            builder.authentication(
                AuthenticationFactory.token(tlqcnProperties.getClient().getAuthentication().getToken())
            );
        }

        // 配置io线程数，默认为CPU核数。
        if (tlqcnProperties.getClient().getInThreads() != null) {
            builder.ioThreads(tlqcnProperties.getClient().getInThreads());
        }

        // 配置监听器线程数，默认为CPU核数。使用同一客户端的消费者的监听器共享此线程池，确保一个消费者只能使用一个线程。
        if (tlqcnProperties.getClient().getListenerThreads() != null) {
            builder.listenerThreads(tlqcnProperties.getClient().getListenerThreads());
        }

        return builder.build();
    }

    @Bean
    public ExecutorService perfTestExecutorService() {
        return Executors.newCachedThreadPool(new DefaultThreadFactory("tlqcn-perf-test"));
    }

}