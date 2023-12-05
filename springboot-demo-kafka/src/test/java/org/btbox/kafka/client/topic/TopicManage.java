package org.btbox.kafka.client.topic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.btbox.kafka.client.common.CommonConstant;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/5 16:51
 * @version: 1.0
 */
public class TopicManage {

    @Test
    public void createTopic() {
        Properties pro = new Properties();
        pro.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                CommonConstant.CLUSTER_SERVICE_URL);

        NewTopic newTopic = new NewTopic("topic_e", 5, (short) 1);

        AdminClient adminClient = AdminClient.create(pro);
        List<NewTopic> topicList = Arrays.asList(newTopic);
        adminClient.createTopics(topicList);

        adminClient.close();

    }

}