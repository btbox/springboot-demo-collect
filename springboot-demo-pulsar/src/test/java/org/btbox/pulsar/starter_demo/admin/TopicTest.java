package org.btbox.pulsar.starter_demo.admin;

import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.pulsar.core.PulsarAdministration;

import java.util.List;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_8080;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 14:58
 * @version: 1.0
 */
@SpringBootTest
public class TopicTest {

    @Autowired
    private PulsarAdministration pulsarAdministration;
    
    /**
     * 创建topic
     */
    @Test
    @SneakyThrows
    public void createTopic() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();
        // 创建无分区topic
        pulsarAdmin.topics().createNonPartitionedTopic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_user");
        // 创建临时topic
        // pulsarAdmin.topics().createNonPartitionedTopic("non-persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl");
        // 创建分区topic
        // pulsarAdmin.topics().createPartitionedTopic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl", 5);

        pulsarAdmin.close();
    }

    /**
     * topic列表
     */
    @Test
    @SneakyThrows
    public void listTopic() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();
        // 创建无分区topic
        List<String> topics = pulsarAdmin.topics().getList("btbox_pulsar_t/btbox_pulsar_n");
        for (String topic : topics) {
            System.out.println("topic = " + topic);
        }
        pulsarAdmin.close();
    }

    /**
     * 修改分片数topic
     */
    @Test
    @SneakyThrows
    public void updateTopic() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();
        pulsarAdmin.topics().updatePartitionedTopic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl", 6);
        pulsarAdmin.close();
    }

    /**
     * topic信息
     */
    @Test
    @SneakyThrows
    public void topicInfo() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();
        PartitionedTopicMetadata partitionedTopicMetadata = pulsarAdmin.topics().getPartitionedTopicMetadata("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl");
        System.out.println("分区数量 = " + partitionedTopicMetadata.partitions);
        System.out.println("属性 = " + partitionedTopicMetadata.properties);

        pulsarAdmin.close();
    }

    /**
     * 删除topic
     */
    @Test
    @SneakyThrows
    public void deleteTopic() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();
        pulsarAdmin.topics().delete("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl");
        pulsarAdmin.close();
    }
}