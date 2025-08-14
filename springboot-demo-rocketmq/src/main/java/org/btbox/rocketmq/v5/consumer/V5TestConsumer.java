// package org.btbox.rocketmq.v5.consumer;
//
// import lombok.extern.slf4j.Slf4j;
// import org.apache.rocketmq.client.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
// import org.apache.rocketmq.client.apis.message.MessageView;
// import org.apache.rocketmq.client.core.RocketMQListener;
// import org.springframework.stereotype.Service;
//
// import java.nio.ByteBuffer;
// import java.nio.charset.StandardCharsets;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/25 09:37
//  * @version: 1.0
//  */
// @Slf4j
// @Service
// @RocketMQMessageListener(
//         topic = "normalTopic",
//         tag = "*",
//         consumerGroup = "v5-boot-consumer-group",
//         endpoints = "10.1.1.72:18081",
//         accessKey = "rocketmq",
//         secretKey = "12345678"
// )
// public class V5TestConsumer implements RocketMQListener {
//
//     @Override
//     public ConsumeResult consume(MessageView messageView) {
//         System.out.println("消息:" + messageView);
//         ByteBuffer body = messageView.getBody();
//         String message = StandardCharsets.UTF_8.decode(body).toString();
//         log.info("消息, message={}", message);
//         return ConsumeResult.SUCCESS;
//     }
//
// }