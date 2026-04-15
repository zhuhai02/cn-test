package com.tongtech.cntest.service;

import com.tongtech.cntest.service.api.AdminService;
import com.tongtech.tlqcn.client.admin.TlqcnAdmin;
import com.tongtech.tlqcn.client.admin.TlqcnAdminException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AdminServiceImpl implements AdminService {

    private final TlqcnAdmin tlqcnAdmin;

    @Autowired
    public AdminServiceImpl(TlqcnAdmin tlqcnAdmin) {
        this.tlqcnAdmin = tlqcnAdmin;
    }

    @Override
    public boolean clearAndCreateTopic(String topic) {
        try {
            tlqcnAdmin.topics().delete(topic, true);
            log.info("删除topic<{}>成功", topic);
        } catch (TlqcnAdminException e) {
            log.warn("删除topic<{}>失败或不存在: {}", topic, e.getMessage());
        }

        try {
            tlqcnAdmin.topics().createNonPartitionedTopic(topic);
            log.info("创建topic<{}>成功", topic);
            return true;
        } catch (TlqcnAdminException e) {
            log.error("创建topic<{}>异常", topic, e);
            return false;
        }
    }

    @Override
    public boolean clearAndCreatePartitionTopic(String topic, int partitionNum) {
        try {
            tlqcnAdmin.topics().deletePartitionedTopic(topic, true);
            log.info("删除topic<{}>成功", topic);
        } catch (TlqcnAdminException e) {
            log.warn("删除topic<{}>失败或不存在: {}", topic, e.getMessage());
        }

        try {
            tlqcnAdmin.topics().createPartitionedTopic(topic, partitionNum);
            log.info("创建topic<{}>成功，分区数: {}", topic, partitionNum);
            return true;
        } catch (TlqcnAdminException e) {
            log.error("创建topic<{}>异常", topic, e);
            return false;
        }
    }

    @Override
    public TlqcnAdmin getTlqcnAdmin() {
        return tlqcnAdmin;
    }
}
