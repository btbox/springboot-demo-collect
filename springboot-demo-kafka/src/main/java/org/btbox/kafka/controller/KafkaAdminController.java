package org.btbox.kafka.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/12 9:34
 * @version: 1.0
 */
@RestController
@Slf4j
@RequiredArgsConstructor
public class KafkaAdminController {
    
    private final KafkaAdmin kafkaAdmin;

    @GetMapping("create-topic")
    public void createTopic() {
        NewTopic newTopic = new NewTopic("topic_g", 5, (short) 2);
        kafkaAdmin.createOrModifyTopics(newTopic);
    }
    
}