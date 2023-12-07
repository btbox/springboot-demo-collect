package org.btbox.kafka.controller;

import org.apache.kafka.clients.consumer.Consumer;
import org.btbox.kafka.domain.entity.Person;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/4 10:49
 * @version: 1.0
 */
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = "topic_f")
    public void consumerTopic(List<Person> msgs) {

        System.out.println("开始消费= ");

        for (Person msg : msgs) {
            System.out.println("msg = " + msg);
        }

    }

    /**
     * 手动提交offset
     * @param msgs
     * @param ack
     */
    // @KafkaListener(topics = "topic_f")
    // public void consumerTopicAck(List<Person> msgs, Acknowledgment ack) {
    //
    //     System.out.println("开始消费= ");
    //
    //     for (Person msg : msgs) {
    //         System.out.println("msg = " + msg);
    //     }
    //
    //     ack.acknowledge();
    //
    //
    // }

}