// package org.btbox.rocketmq.consumer;
//
// import org.apache.rocketmq.common.message.MessageExt;
// import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.spring.annotation.SelectorType;
// import org.apache.rocketmq.spring.core.RocketMQListener;
// import org.springframework.stereotype.Component;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/14 16:58
//  * @version: 1.0
//  */
// @Component
// @RocketMQMessageListener(
//         topic = "traceTopic",
//         consumerGroup = "boot-trace-consumer-group",
//         enableMsgTrace = true   // 开启消费消息轨迹
// )
// public class BootTraceMsgListener implements RocketMQListener<MessageExt> {
//     @Override
//     public void onMessage(MessageExt message) {
//         System.out.println("消费消息: " + new String(message.getBody()));
//     }
// }