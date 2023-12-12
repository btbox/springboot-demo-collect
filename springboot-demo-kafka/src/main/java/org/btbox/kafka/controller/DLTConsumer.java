package org.btbox.kafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.btbox.kafka.domain.entity.Person;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @description: 死信队列
 * @author: BT-BOX
 * @createDate: 2023/12/11 15:17
 * @version: 1.0
 */
@Slf4j
@Component
public class DLTConsumer {

    // @RetryableTopic()
    // @KafkaListener(topics = "topic-dlt-test")
    // public void onMessage(List<Person> msgs, Acknowledgment ack) {
    //     for (Person msg : msgs) {
    //         log.info("[onMessage][消息内容：{}]", msg);
    //     }
    //     ack.acknowledge();
    //     // throw new RuntimeException("test kafka exception");
    // }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 3000, multiplier = 0)
    )
    @KafkaListener(topics = "topic-dlt-test")
    public void t(Person msgs, Acknowledgment ack) {
        log.info("[onMessage][消息内容：{}]", msgs);
        // ack.acknowledge();
        throw new RuntimeException("test kafka exception");
    }

    @DltHandler
    public void processMessage(Person msgs, Acknowledgment ack) {
        log.info("进入死信队列,msg:{}", msgs);
        ack.acknowledge();
    }

}