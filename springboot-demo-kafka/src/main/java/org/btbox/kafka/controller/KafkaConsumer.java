package org.btbox.kafka.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/4 10:49
 * @version: 1.0
 */
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = "first")
    public void consumerTopic(String msg) {
        System.out.println("消息: " + msg);
    }

}