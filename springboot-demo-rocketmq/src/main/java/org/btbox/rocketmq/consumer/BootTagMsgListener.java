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
//  * @createDate: 2025/7/14 14:23
//  * @version: 1.0
//  */
// @Component
// @RocketMQMessageListener(
//         topic = "bootTagTopic",
//         consumerGroup = "boot-tag-consumer-group",
//         selectorType = SelectorType.TAG,   // Tag模式
//         selectorExpression = "tagA || tagB" // * 号则代表全部
//         // selectorType = SelectorType.SQL92, // sql92模式
//         // selectorExpression = "a in (3,5,7)" // 表达式，sql92开启需要broker.conf中开启enablePropertyFilter=true
//
// )
// public class BootTagMsgListener implements RocketMQListener<MessageExt> {
//     @Override
//     public void onMessage(MessageExt message) {
//         System.out.println(new String(message.getBody()));
//     }
// }