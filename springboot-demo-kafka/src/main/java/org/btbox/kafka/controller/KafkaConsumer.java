// package org.btbox.kafka.controller;
//
// import lombok.extern.slf4j.Slf4j;
// import org.apache.kafka.clients.consumer.Consumer;
// import org.btbox.kafka.domain.entity.Person;
// import org.springframework.context.annotation.Configuration;
// import org.springframework.kafka.annotation.KafkaListener;
// import org.springframework.kafka.annotation.PartitionOffset;
// import org.springframework.kafka.annotation.TopicPartition;
// import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
// import org.springframework.kafka.support.Acknowledgment;
// import org.springframework.transaction.annotation.Transactional;
//
// import java.util.List;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2023/12/4 10:49
//  * @version: 1.0
//  */
// @Configuration
// @Slf4j
// public class KafkaConsumer {
//
//     // @KafkaListener(topics = "topic_f")
//     // public void consumerTopic(List<Person> msgs) {
//     //
//     //     System.out.println("开始消费= ");
//     //
//     //     for (Person msg : msgs) {
//     //         System.out.println("msg = " + msg);
//     //     }
//     //
//     // }
//
//     /**
//      * 手动提交offset
//      * @param msgs
//      * @param ack
//      */
//     @KafkaListener(topics = "topic_g")
//     public void consumerTopicAck(List<Person> msgs, Acknowledgment ack) {
//
//         System.out.println("开始消费= ");
//
//         for (Person msg : msgs) {
//             System.out.println("msg = " + msg);
//         }
//
//         // ack.acknowledge();
//
//
//     }
//     //
//     // /**
//     //  * 指定offset消费
//     //  */
//     // @KafkaListener(topicPartitions = {
//     //         @TopicPartition(topic = "topic_f", partitionOffsets = {
//     //                 @PartitionOffset(partition = "0", initialOffset = "0")
//     //         })
//     // })
//     // public void specifyPartition(List<Person> msgs) {
//     //
//     //     System.out.println("开始消费= ");
//     //
//     //     for (Person msg : msgs) {
//     //         System.out.println("msg = " + msg);
//     //     }
//     //
//     // }
//
//
//     // /**
//     //  * 消费异常
//     //  */
//     // @KafkaListener(topics = "topic_f",errorHandler = "myConsumerAwareErrorHandler")
//     // public void errorHandler(List<Person> msgs, Acknowledgment ack) {
//     //     log.info("开始消费");
//     // }
//     //
//     // /**
//     //  * 指定某个factory
//     //  */
//     // @KafkaListener(topics = "topic_f", containerFactory = "kafkaListenerContainerFactory2")
//     // public void containerFactory(List<Person> msgs, Acknowledgment ack) {
//     //     log.info("开始消费");
//     // }
//
// }