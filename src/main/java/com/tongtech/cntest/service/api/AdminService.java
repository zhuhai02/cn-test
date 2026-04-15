package com.tongtech.cntest.service.api;

import com.tongtech.tlqcn.client.admin.TlqcnAdmin;

public interface AdminService {

    /**
     * topic 重建
     * @param topic
     * @return
     */
     boolean clearAndCreateTopic(String topic);

    /**
     * 分区 topic 重建
     * @param topic
     * @param partitionNum 分区数
     * @return
     */
    boolean clearAndCreatePartitionTopic(String topic, int partitionNum);

     TlqcnAdmin getTlqcnAdmin();
}
