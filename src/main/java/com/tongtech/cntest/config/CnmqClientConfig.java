package com.tongtech.cntest.config;


import com.tongtech.cnmq.client.admin.CnmqAdmin;
import com.tongtech.cnmq.client.api.AuthenticationFactory;
import com.tongtech.cnmq.client.api.ClientBuilder;
import com.tongtech.cnmq.client.api.CnmqClient;
import com.tongtech.cnmq.client.api.CnmqClientException;
import com.tongtech.cnmq.shade.io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class CnmqClientConfig {

    private final CnmqProperties cnmqProperties;



    @Bean
    public CnmqAdmin cnmqAdmin() throws CnmqClientException {
        return CnmqAdmin.builder()
                .serviceHttpUrl(cnmqProperties.getClient().getServiceHttpUrl())
                .build();

    }
    /**
     * 单例类型CnmqClient，适用于生产者的创建，和使用receive接口消费的消费者使用
     * @return
     * @throws CnmqClientException
     */
    @Bean
    public CnmqClient cnmqClient() throws CnmqClientException {
        if (cnmqProperties.getClient() == null || cnmqProperties.getClient().getServiceUrl() == null) {
            return null;
        }
        ClientBuilder builder = CnmqClient.builder()
                // 配置TongLINK/Q-CN服务器地址
                .serviceUrl(cnmqProperties.getClient().getServiceUrl());

        // 配置认证
        if (cnmqProperties.getClient().getAuthentication().isEnabled()) {
            builder.authentication(
                AuthenticationFactory.token(cnmqProperties.getClient().getAuthentication().getToken())
            );
        }

        // 配置io线程数，默认为CPU核数。
        if (cnmqProperties.getClient().getInThreads() != null) {
            builder.ioThreads(cnmqProperties.getClient().getInThreads());
        }

        // 配置监听器线程数，默认为CPU核数。使用同一客户端的消费者的监听器共享此线程池，确保一个消费者只能使用一个线程。
        if (cnmqProperties.getClient().getListenerThreads() != null) {
            builder.listenerThreads(cnmqProperties.getClient().getListenerThreads());
        }

        return builder.build();
    }

    @Bean
    public ExecutorService perfTestExecutorService() {
        return Executors.newCachedThreadPool(new DefaultThreadFactory("cnmq-perf-test"));
    }

}