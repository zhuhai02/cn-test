package com.tongtech.cntest.service;

import com.tongtech.cntest.config.TlqcnProperties;
import com.tongtech.cntest.service.api.KotTestService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class KotTestServiceImpl implements KotTestService {
    private static final Logger log = LoggerFactory.getLogger("PROTOCOL_TEST_LOGGER");

    private static final Marker PROTOCOL_TEST = MarkerFactory.getMarker("PROTOCOL_TEST");

    private final TlqcnProperties tlqcnProperties;

    private String url;

    @Autowired
    public KotTestServiceImpl(TlqcnProperties tlqcnProperties) {
        this.tlqcnProperties = tlqcnProperties;
        this.url = tlqcnProperties.getKotTestConfig().getUrl();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 优化配置
        props.put(ProducerConfig.ACKS_CONFIG, "all");  // 所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3);  // 重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);  // 批次大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);  // 等待时间
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // 缓冲区大小

        return new KafkaProducer<>(props);
    }


    @Override
    public String syncSendTest(String topic, int msgNum) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------kafka协议同步发送测试开始,测试编号[{}]----------", testNum);

        try {
            KafkaProducer<String, String> producer = createProducer();

            for (int i = 0; i < msgNum; i++) {
                Date date = new Date();
                String msgStr = "Kafka协议同步发送消息，num:" + i + ",time:" + date.getTime();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgStr);
                RecordMetadata metadata = producer.send(record).get();
                log.info(PROTOCOL_TEST, msgStr + "，partition：<{}>，offset：<{}>", metadata.partition(), metadata.offset());
            }

            log.info(PROTOCOL_TEST, "kafka协议同步发送测试完成");
            producer.close();
        } catch (Exception e) {
            log.error(PROTOCOL_TEST, "kafka协议同步发送测试失败，topic：<{}>", topic, e);
        }
        log.info(PROTOCOL_TEST, "----------kafka协议同步发送测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }

    @Override
    public String simpleConsumerTest(String topic, int msgNum) {
        String testNum = UUID.randomUUID().toString();
        log.info(PROTOCOL_TEST, "----------kafka协议简单消费测试开始,测试编号[{}]----------", testNum);
        // 1. 设置消费者配置参数
        Properties props = new Properties();
        // Kafka 集群地址，多个用逗号分隔
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        // 消费者组ID，同一组内的消费者共同消费订阅的主题
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");
        // 键和值的反序列化器
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 从何处开始消费: earliest(最早偏移量), latest(最新), none
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 是否自动提交偏移量，默认为true
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交偏移量的间隔时间
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // 2. 创建 Kafka 消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // 3. 订阅主题（可以订阅一个或多个）
            consumer.subscribe(Collections.singletonList(topic));

            KafkaProducer<String, String> producer = createProducer();

            for (int i = 0; i < msgNum; i++) {
                Date date = new Date();
                String msgStr = "Kafka协议发送消息，num:" + i + ",time:" + date.getTime();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, msgStr);
                RecordMetadata metadata = producer.send(record).get();
                log.info(PROTOCOL_TEST, msgStr + "，partition：<{}>，offset：<{}>", metadata.partition(), metadata.offset());
            }

            int i=0;

            while (i<20) {
                i++;
                // 拉取消息，超时时间为1000毫秒
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(PROTOCOL_TEST, "Kafka协议消费成功：<{}>", record.value());
                }

                consumer.commitSync();

                Thread.sleep(1000 * 3);
            }

            log.info(PROTOCOL_TEST, "kafka协议简单消费测试完成");
            producer.close();
        } catch (ExecutionException e) {
            log.error(PROTOCOL_TEST,"kafka协议简单消费测试失败",e);
        } catch (InterruptedException e) {
            log.error(PROTOCOL_TEST,"kafka协议简单消费测试失败",e);
        }
        log.info(PROTOCOL_TEST, "----------kafka协议简单消费测试结束,测试编号[{}]----------", testNum);
        return "测试完毕，请查看logs/protocol_test.log中测试编号[" + testNum + "]之间的日志";
    }
}
